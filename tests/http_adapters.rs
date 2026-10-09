#![cfg(all(
    any(feature = "services-s3", feature = "services-webdav"),
    any(
        feature = "http-transport-reqwest-rustls",
        feature = "http-transport-reqwest-native-tls"
    )
))]

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use opendal_sync::Storage;

// A real loopback HTTP exchange catches missing default transport installation,
// which constructing an operator or testing Memory alone cannot detect.
fn serve_once(status: &str, body: &str) -> anyhow::Result<(String, thread::JoinHandle<String>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "HTTP adapter sent no request");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        stream.write_all(response.as_bytes()).unwrap();
        String::from_utf8(request).unwrap()
    });
    Ok((endpoint, server))
}

#[cfg(feature = "services-s3")]
#[tokio::test]
async fn s3_lists_size_and_unix_timestamp_over_http() -> anyhow::Result<()> {
    let (endpoint, server) = serve_once(
        "200 OK",
        r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test</Name><IsTruncated>false</IsTruncated><Contents><Key>file.txt</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>"etag"</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"#,
    )?;
    let storage = opendal_sync::storage::S3Storage::new(
        "test",
        "auto",
        "test-key",
        "test-secret",
        Some(&endpoint),
        None,
    )?;
    let files = storage.list_files(None).await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "file.txt");
    assert_eq!(files[0].size, 7);
    assert_eq!(files[0].modified_time, 1_704_067_200);
    assert!(!files[0].is_dir);
    assert!(server.join().unwrap().starts_with("GET /test?"));
    Ok(())
}

#[cfg(feature = "services-webdav")]
#[tokio::test]
async fn webdav_lists_size_and_unix_timestamp_over_http() -> anyhow::Result<()> {
    let (endpoint, server) = serve_once(
        "207 Multi-Status",
        r#"<d:multistatus xmlns:d="DAV:"><d:response><d:href>/file.txt</d:href><d:propstat><d:prop><d:resourcetype/><d:getcontentlength>7</d:getcontentlength><d:getlastmodified>Mon, 01 Jan 2024 00:00:00 GMT</d:getlastmodified></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>"#,
    )?;
    let storage = opendal_sync::storage::WebDavStorage::new(&endpoint, "test", "secret", None)?;
    let files = storage.list_files(None).await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "file.txt");
    assert_eq!(files[0].size, 7);
    assert_eq!(files[0].modified_time, 1_704_067_200);
    assert!(!files[0].is_dir);
    assert!(server.join().unwrap().starts_with("PROPFIND / HTTP/1.1"));
    Ok(())
}
