pub mod runtime {}

pub mod task {
    use std::{any::Any, future::Future};

    use futures::FutureExt;

    #[derive(Debug)]
    pub enum JoinError {}

    impl std::fmt::Display for JoinError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("join error")
        }
    }

    impl std::error::Error for JoinError {}

    impl JoinError {
        /// Consumes the join error, returning the object with which the task panicked.
        #[track_caller]
        pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
            self.try_into_panic().expect("`JoinError` reason is not a panic.")
        }

        /// Consumes the join error, returning the object with which the task
        /// panicked if the task terminated due to a panic. Otherwise, `self` is
        /// returned.
        pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
            Err(self)
        }
    }

    pub struct JoinHandle<T> {
        inner: Option<wstd::runtime::Task<T>>,
    }

    #[allow(unsafe_code)]
    unsafe impl<T> Send for JoinHandle<T> {}

    #[allow(unsafe_code)]
    unsafe impl<T> Sync for JoinHandle<T> {}

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(
            self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let this = self.get_mut();
            let result = futures::ready!(this.inner.as_mut().unwrap().poll_unpin(cx));
            std::task::Poll::Ready(Ok(result))
        }
    }

    impl<T> Drop for JoinHandle<T> {
        fn drop(&mut self) {
            self.inner.take().unwrap().detach();
        }
    }

    pub fn spawn_blocking<F, R>(_f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        panic!("wstd does not support spawn_blocking");
    }

    pub fn spawn<F: Future + 'static>(future: F) -> JoinHandle<<F as Future>::Output> {
        let task = wstd::runtime::spawn(future);
        JoinHandle { inner: Some(task) }
    }

    pub fn block_on<F: Future>(future: F) -> F::Output {
        wstd::runtime::block_on(future)
    }
}

pub mod time {
    use std::{
        future::{Future, IntoFuture},
        pin::Pin,
    };

    use futures::FutureExt;

    pub struct Sleep {
        inner: wstd::time::Wait,
    }

    impl From<wstd::time::Wait> for Sleep {
        fn from(value: wstd::time::Wait) -> Self {
            Self { inner: value }
        }
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            futures::ready!(self.get_mut().inner.poll_unpin(cx));
            std::task::Poll::Ready(())
        }
    }

    #[allow(unsafe_code)]
    unsafe impl Send for Sleep {}

    #[allow(unsafe_code)]
    unsafe impl Sync for Sleep {}

    pub fn sleep(duration: std::time::Duration) -> Sleep {
        wstd::time::Timer::after(duration.into()).wait().into()
    }

    pub struct Timeout<F> {
        inner: wstd::future::Timeout<Pin<Box<F>>, Sleep>,
    }

    #[allow(unsafe_code)]
    unsafe impl<F> Send for Timeout<F> {}

    #[allow(unsafe_code)]
    unsafe impl<F> Sync for Timeout<F> {}

    impl<F: Future> Future for Timeout<F> {
        type Output = Result<<F as Future>::Output, error::Elapsed>;

        fn poll(
            self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            let this = self.get_mut();
            let result = futures::ready!(this.inner.poll_unpin(cx));

            std::task::Poll::Ready(result.map_err(|_| error::Elapsed(())))
        }
    }

    pub fn timeout<F: IntoFuture>(
        duration: std::time::Duration, fut: F,
    ) -> Timeout<<F as IntoFuture>::IntoFuture> {
        let deadline = sleep(duration);
        let timeout = wstd::future::FutureExt::timeout(Box::pin(fut.into_future()), deadline);
        Timeout { inner: timeout }
    }

    pub mod error {
        #[derive(Debug)]
        pub struct Elapsed(pub(crate) ());

        impl std::fmt::Display for Elapsed {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("deadline has elapsed")
            }
        }

        impl std::error::Error for Elapsed {}
    }
}
