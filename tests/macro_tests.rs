use lazyflow::prelude::*;
use lazyflow::{pipe, pipe_gen, pipe_gen_once};

#[tokio::test]
async fn pipe_macro_basic() {
    let result = pipe![1, 2, 3].collect().await.unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn pipe_macro_trailing_comma() {
    let result = pipe![10, 20,].collect().await.unwrap();
    assert_eq!(result, vec![10, 20]);
}

#[tokio::test]
async fn pipe_macro_single() {
    let result = pipe![42].collect().await.unwrap();
    assert_eq!(result, vec![42]);
}

#[tokio::test]
async fn pipe_macro_chained() {
    let result = pipe![1, 2, 3, 4, 5]
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![20, 40]);
}

#[tokio::test]
async fn pipe_gen_basic() {
    let result = pipe_gen!(tx => {
        tx.emit(1).await?;
        tx.emit(2).await?;
        tx.emit(3).await?;
    })
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn pipe_gen_with_loop() {
    let result = pipe_gen!(tx => {
        for i in 0..5 {
            tx.emit(i * 10).await?;
        }
    })
    .map(|x| x + 1)
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![1, 11, 21, 31, 41]);
}

#[derive(Debug)]
struct Double;

#[operator]
impl Double {
    async fn execute(&self, input: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(input * 2)
    }
}

#[tokio::test]
async fn operator_macro_basic() {
    let result = pipe![1, 2, 3].pipe(Double).collect().await.unwrap();
    assert_eq!(result, vec![2, 4, 6]);
}

#[derive(Debug)]
struct Prefix {
    prefix: String,
}

#[operator]
impl Prefix {
    async fn execute(
        &self,
        input: String,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(format!("{}{}", self.prefix, input))
    }
}

#[tokio::test]
async fn operator_macro_with_state() {
    let op = Prefix {
        prefix: "hi_".to_string(),
    };
    let result = pipe!["a".to_string(), "b".to_string()]
        .pipe(op)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec!["hi_a", "hi_b"]);
}

struct Counter {
    items: Vec<i64>,
    done: bool,
}

#[pull_operator]
impl Counter {
    async fn next_chunk(&mut self) -> Result<Option<Vec<i64>>, lazyflow::pull::PipeError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(Some(self.items.clone()))
    }
}

#[tokio::test]
async fn pull_operator_macro_basic() {
    let src = Counter {
        items: vec![10, 20, 30],
        done: false,
    };
    let result = Pipe::from_pull_once(src).collect().await.unwrap();
    assert_eq!(result, vec![10, 20, 30]);
}

struct Countdown {
    n: usize,
}

#[pull_operator]
impl Countdown {
    async fn next_chunk(&mut self) -> Result<Option<Vec<usize>>, lazyflow::pull::PipeError> {
        if self.n == 0 {
            return Ok(None);
        }
        let chunk: Vec<usize> = (1..=self.n).collect();
        self.n = 0;
        Ok(Some(chunk))
    }
}

#[tokio::test]
async fn pull_operator_macro_with_pipe_ops() {
    let result = Pipe::from_pull_once(Countdown { n: 5 })
        .filter(|x| x % 2 != 0)
        .map(|x| x * 100)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![100, 300, 500]);
}

#[tokio::test]
async fn pipe_gen_once_basic() {
    let owned_data = vec![10, 20, 30];
    let result = pipe_gen_once!(tx => {
        for item in owned_data {
            tx.emit(item).await?;
        }
    })
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![10, 20, 30]);
}

#[tokio::test]
async fn pipe_gen_once_with_operators() {
    let owned_data = vec![1, 2, 3, 4, 5];
    let result = pipe_gen_once!(tx => {
        for item in owned_data {
            tx.emit(item).await?;
        }
    })
    .filter(|x| x % 2 != 0)
    .map(|x| x * 100)
    .collect()
    .await
    .unwrap();
    assert_eq!(result, vec![100, 300, 500]);
}

#[tokio::test]
async fn pipe_gen_once_errors_on_double_materialize() {
    let owned_data = vec![1, 2, 3];
    let pipe = pipe_gen_once!(tx => {
        for item in owned_data {
            tx.emit(item).await?;
        }
    });
    let clone = pipe.clone();
    let _ = pipe.collect().await;
    let result = clone.collect().await;
    assert!(
        result.is_err(),
        "second materialization should return an error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_for_each_basic() {
    use std::sync::{Arc, Mutex};
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    pipe![1, 2, 3]
        .eval_for_each(move |x| {
            let seen = seen2.clone();
            async move {
                seen.lock().unwrap().push(x);
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_for_each_with_async_io() {
    use std::sync::{Arc, Mutex};
    let results = Arc::new(Mutex::new(Vec::new()));
    let results2 = results.clone();
    pipe![10, 20, 30]
        .map(|x| x + 1)
        .eval_for_each(move |x| {
            let results = results2.clone();
            async move {
                tokio::task::yield_now().await;
                results.lock().unwrap().push(x);
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(*results.lock().unwrap(), vec![11, 21, 31]);
}

#[lazyflow::pipe_fn]
async fn triple(x: i64) -> i64 {
    x * 3
}

#[tokio::test]
async fn pipe_fn_basic() {
    let result = pipe![1, 2, 3].eval_map(triple).collect().await.unwrap();
    assert_eq!(result, vec![3, 6, 9]);
}

#[lazyflow::pipe_fn]
async fn add_prefix(s: String) -> String {
    format!("hello_{s}")
}

#[tokio::test]
async fn pipe_fn_type_change() {
    let result = pipe!["a".to_string(), "b".to_string()]
        .eval_map(add_prefix)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec!["hello_a", "hello_b"]);
}

#[lazyflow::pipe_fn]
async fn to_upper_case(s: String) -> String {
    s.to_uppercase()
}

#[tokio::test]
async fn pipe_fn_chained() {
    let result = pipe!["foo".to_string(), "bar".to_string()]
        .eval_map(to_upper_case)
        .eval_map(add_prefix)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec!["hello_FOO", "hello_BAR"]);
}

#[lazyflow::pipe_fn]
async fn parse_int(s: String) -> lazyflow::PipeResult<i64> {
    Ok(s.parse()?)
}

#[tokio::test]
async fn pipe_fn_fallible() {
    let result = pipe!["1".to_string(), "2".to_string(), "3".to_string()]
        .eval_map(parse_int)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![1, 2, 3]);
}

#[tokio::test]
async fn pipe_fn_fallible_error() {
    let result = pipe!["1".to_string(), "bad".to_string()]
        .eval_map(parse_int)
        .collect()
        .await;
    assert!(result.is_err());
}

#[lazyflow::pipe_fn]
async fn delayed_double(x: i64) -> i64 {
    tokio::task::yield_now().await;
    x * 2
}

#[tokio::test]
async fn pipe_fn_bare_return_may_await() {
    let result = pipe![1, 2]
        .eval_map(delayed_double)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![2, 4]);
}

#[lazyflow::pipe_fn]
async fn clamp_after_await(x: i64) -> i64 {
    tokio::task::yield_now().await;
    if x > 10 {
        return 10;
    }
    x
}

#[tokio::test]
async fn pipe_fn_bare_return_keeps_early_return_after_await() {
    let result = pipe![5, 50]
        .eval_map(clamp_after_await)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![5, 10]);
}

#[lazyflow::pipe_fn]
async fn parse_after_await(s: String) -> lazyflow::PipeResult<i64> {
    tokio::task::yield_now().await;
    Ok(s.parse()?)
}

#[tokio::test]
async fn pipe_fn_fallible_return_may_await() {
    let result = pipe!["7".to_string()]
        .eval_map(parse_after_await)
        .eval_map(delayed_double)
        .collect()
        .await
        .unwrap();
    assert_eq!(result, vec![14]);
}
