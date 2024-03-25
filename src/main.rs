use clap::{Args, Parser};
use hyper::body::{Body, Bytes, Frame, SizeHint};
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use listenfd::ListenFd;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::{interval, Interval};
use tokio_native_tls::native_tls::Identity;
use tokio_native_tls::TlsAcceptor;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ProgramArgs {
    #[command(flatten)]
    pub perf: PerfArgs,
    #[arg(long)]
    pub public_key: String,
    #[arg(long)]
    pub private_key: String,
}

#[derive(Args, Debug)]
pub struct PerfArgs {
    #[arg(long, default_value = "1.0")]
    pub header_delay: f32,
    #[arg(long, default_value = "2.0")]
    pub chunk_delay: f32,
    #[arg(long, default_value = "256")]
    pub chunk_size: usize,
    #[arg(long, default_value = "4")]
    pub num_chunks: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = ProgramArgs::parse();

    let hyper_builder = Arc::new(hyper_util::server::conn::auto::Builder::new(
        TokioExecutor::new(),
    ));

    let perf_args = Arc::new(args.perf);

    let public_key = std::fs::read(&args.public_key).unwrap();
    let private_key = std::fs::read(&args.private_key).unwrap();

    let acceptor = Arc::new(TlsAcceptor::from(
        tokio_native_tls::native_tls::TlsAcceptor::new(
            Identity::from_pkcs8(&public_key, &private_key).unwrap(),
        )
        .unwrap(),
    ));

    let mut systemd_sockets = ListenFd::from_env();

    let mut listener_join_set = JoinSet::new();

    for i in 0..systemd_sockets.len() {
        if let Some(listener) = systemd_sockets.take_tcp_listener(i).unwrap() {
            listener.set_nonblocking(true).unwrap();

            let tokio_listener = TcpListener::from_std(listener).unwrap();

            listener_join_set.spawn(listen(
                tokio_listener,
                hyper_builder.clone(),
                perf_args.clone(),
                acceptor.clone(),
            ));
        }
    }

    // fail on first exit from a listener
    listener_join_set.join_next().await.unwrap().unwrap();
}

async fn listen(
    listener: TcpListener,
    builder: Arc<hyper_util::server::conn::auto::Builder<TokioExecutor>>,
    args: Arc<PerfArgs>,
    acceptor: Arc<TlsAcceptor>,
) {
    loop {
        let (stream, _addr) = listener.accept().await.unwrap();

        tokio::spawn(serve_connection(
            stream,
            builder.clone(),
            args.clone(),
            acceptor.clone(),
        ));
    }
}

async fn serve_connection(
    stream: TcpStream,
    builder: Arc<hyper_util::server::conn::auto::Builder<TokioExecutor>>,
    args: Arc<PerfArgs>,
    acceptor: Arc<TlsAcceptor>,
) {
    let tls = acceptor.accept(stream).await.unwrap();

    let sleep_duration = Duration::from_secs_f32(args.header_delay);

    builder
        .serve_connection(
            TokioIo::new(tls),
            service_fn(|_req| {
                let body = ChunkedDelayBody::new(
                    args.num_chunks,
                    Duration::from_secs_f32(args.chunk_delay),
                    args.chunk_size,
                );

                async move {
                    tokio::time::sleep(sleep_duration).await;

                    Response::builder().status(StatusCode::OK).body(body)
                }
            }),
        )
        .await
        .unwrap()
}

struct ChunkedDelayBody {
    remaining_chunks: usize,
    delay: Interval,
    chunk: Bytes,
}

impl ChunkedDelayBody {
    fn new(n_chunks: usize, delay: Duration, chunk_size: usize) -> Self {
        Self {
            remaining_chunks: n_chunks,
            delay: interval(delay),
            chunk: Bytes::from(vec![b'a'; chunk_size]),
        }
    }
}

impl Body for ChunkedDelayBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        if this.is_end_stream() {
            return Poll::Ready(None);
        }

        ready!(this.delay.poll_tick(cx));

        this.remaining_chunks -= 1;

        Poll::Ready(Some(Ok(Frame::data(this.chunk.clone()))))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining_chunks == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.chunk.len() as u64 * self.remaining_chunks as u64)
    }
}
