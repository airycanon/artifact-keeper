//! Transport regression for h2 0.4.16 (hyperium/h2#939).
//! A redirect drops its small response body. Repeating that on one connection
//! must not exhaust the DATA-frame budget and generate a local GOAWAY.

#[cfg(ak_test_shard = "services-1")]
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test]
    async fn h2_small_redirect_bodies_do_not_exhaust_frame_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let peer_errors = Arc::new(Mutex::new(Vec::new()));
        let observed_errors = peer_errors.clone();
        let server = tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
                let observed_errors = observed_errors.clone();
                peers.spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(request) = connection.accept().await {
                        let (request, mut respond) = match request {
                            Ok(request) => request,
                            Err(error) => {
                                observed_errors.lock().unwrap().push(format!("{error:?}"));
                                break;
                            }
                        };
                        let redirect = request.uri().path() == "/redirect";
                        let mut response =
                            http::Response::builder().status(if redirect { 302 } else { 200 });
                        if redirect {
                            response = response.header("location", "/tarball");
                        }
                        let body = if redirect {
                            Bytes::from(vec![b'r'; 88])
                        } else {
                            Bytes::from_static(b"tarball")
                        };
                        let response = response
                            .header("content-length", body.len())
                            .body(())
                            .unwrap();
                        let Ok(mut stream) = respond.send_response(response, false) else {
                            break;
                        };
                        if stream.send_data(body, true).is_err() {
                            break;
                        }
                    }
                });
            }
        });

        // h2c makes the protocol deterministic without TLS certificates, external
        // DNS or proxy environment variables. Redirect handling is reqwest's own.
        let client = reqwest::Client::builder()
            .no_proxy()
            .http2_prior_knowledge()
            .build()
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            for index in 0..512 {
                let response = client
                    .get(format!("http://{address}/redirect"))
                    .send()
                    .await
                    .unwrap_or_else(|error| panic!("redirect {index} failed: {error:?}"));
                assert_eq!(response.version(), reqwest::Version::HTTP_2);
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                assert_eq!(response.bytes().await.unwrap(), "tarball");
            }
        })
        .await;
        server.abort();
        let _ = server.await;
        result.expect("redirect loop must finish without a stalled connection");
        assert!(
            peer_errors.lock().unwrap().is_empty(),
            "HTTP/2 peer observed connection failures: {:?}",
            peer_errors.lock().unwrap()
        );
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "all redirects must reuse one healthy HTTP/2 connection, without GOAWAY recovery"
        );
    }
}
