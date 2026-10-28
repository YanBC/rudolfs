# Rudo*lfs*

[![Crates.io](https://img.shields.io/crates/v/rudolfs?style=for-the-badge)](https://crates.io/crates/rudolfs)
[![AUR version](https://img.shields.io/aur/version/rudolfs?style=for-the-badge)](https://aur.archlinux.org/packages/rudolfs/)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/jasonwhite0/rudolfs?label=Docker&sort=semver&style=for-the-badge)](https://hub.docker.com/r/jasonwhite0/rudolfs)
[![Docker Image Size (latest semver)](https://img.shields.io/docker/image-size/jasonwhite0/rudolfs?sort=semver&style=for-the-badge)](https://hub.docker.com/r/jasonwhite0/rudolfs)

A high-performance, caching Git LFS server with an AWS S3 back-end.

## Features

 - Multiple backends:

   1. AWS S3 backend with an optional local disk cache.
   2. Local disk backend.
   3. SF Express OSS (Swift auth v1) with an optional local disk cache.

 - A configurable local disk cache to speed up downloads (and reduce your
   S3 bill).

 - Corruption-resilient local disk cache. Even if the disk is getting
   blasted by cosmic rays, it'll find corrupted LFS objects and purge them from
   the cache transparently. The client should never notice this happening.

 - Encryption of LFS objects in both the cache and in permanent storage.

 - Separation of GitHub organizations and projects. Just specify the org and
   project names in the URL and they are automatically created. If two projects
   share many LFS objects, have them use the same URL to save on storage space.

 - A tiny (&lt;10MB) Docker image ([jasonwhite0/rudolfs][]).

[jasonwhite0/rudolfs]: https://hub.docker.com/r/jasonwhite0/rudolfs

The back-end storage code is very modular and composable. PRs for implementing
other storage back-ends are welcome. If you begin working on this, please let us
know by submitting an issue.

## Non-Features

 - There is no client authentication. This is meant to be run in an internal
   network with clients you trust, not on the internet with malicious actors.

## Running It

### Generate an encryption key (optional)

If configured, all LFS objects are encrypted with the xchacha20 symmetric stream
cipher. You must generate a 32-byte encryption key before starting the server.

Generating a random key is easy:

    openssl rand -hex 32

Keep this secret and save it in a password manager so you don't lose it. We will
pass this to the server below via the `--key` option. If the `--key` option is
**not** specified, then the LFS objects are **not** encrypted.

**Note**:
 - If the key ever changes (or if encryption is disabled), all existing LFS
   objects will become garbage. When the Git LFS client attempts to download
   them, the SHA256 verification step will fail.
 - Likewise, if encryption is later enabled after it has been disabled, all
   existing unencrypted LFS objects will be seen as garbage.
 - LFS objects in both the cache and in permanent storage are encrypted.
   However, objects are decrypted before being sent to the LFS client, so take
   any necessary precautions to keep your intellectual property safe.

### Development

For testing during development, it is easiest to run it with Cargo. Create
a file called `test.sh` (this path is already ignored by `.gitignore`):

```bash
# Your AWS credentials.
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
export AWS_DEFAULT_REGION=us-west-1

# Change this to the output of `openssl rand -hex 32`.
KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

cargo run -- \
  --cache-dir cache \
  --host localhost:8080 \
  --max-cache-size 10GiB \
  --key $KEY \
  s3 \
  --bucket foobar
```

If you just need to use the local disk as the backend, use the following bash.

```bash
# Change this to the output of `openssl rand -hex 32`.
export RUDOLFS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

cargo run -- --port 8080 local --path=/data
```

**Note**: Always use a different S3 bucket, cache directory, and encryption key
than what you use in your production environment.

**Warning**: *This server may not be accessible from other machines.* Specifying
`--host localhost:8080` will often bind the server to an internal-only loopback
network interface (i.e., if `localhost` resolves to `127.0.0.1` or `[::1]`).
Thus, to make the server accessible from the outside world, specify `--host
0.0.0.0:8080` or just `--port 8080` (the default IP the server will bind to is
`0.0.0.0`). IP `0.0.0.0` means the server shall try to bind to all available
network interfaces, both internal and external. See
https://github.com/jasonwhite/rudolfs/issues/38#issuecomment-973721511 for more
information.

### SF Express OSS

The `oss` backend implements the HTTP protocol used by `sf-oss-golang` directly
in Rust. It uses the same environment variables and an existing container:

```bash
export OSS_ENDPOINT=https://your-oss-endpoint.example.com
export OSS_NAMESCOPE=YOUR-NAMESCOPE  # without AUTH_
export OSS_CONTAINER=your-lfs-container
export OSS_AUTHUSER=your-auth-user
export OSS_AUTHKEY=your-auth-key
export OSS_PROXY=http://127.0.0.1:6003

cargo run -- --port 8080 --cache-dir cache oss
```

The proxy is optional outside networks that require it. `--proxy` / `OSS_PROXY`
applies to all OSS requests, including authentication. Standard HTTP proxy
environment variables are also supported by the HTTP client.

The subcommand accepts `--endpoint`, `--namescope`, `--container`, `--auth-user`,
and `--auth-key` as alternatives to those environment variables. Prefer environment
variables for credentials. `--prefix` / `RUDOLFS_OSS_PREFIX` defaults to `lfs`;
use `--prefix ''` to omit it. Objects are stored at
`/v1/AUTH_<namescope>/<container>/<prefix>/<org>/<project>/<oid-path>`, using
Rudolfs's usual sharded OID path. Startup authenticates and checks the container
with HEAD; it does not create a container.

Global `--key`, `--cache-dir`, and `--max-cache-size` options work with OSS.
Uploads and downloads stream through Rudolfs, preserving encryption and SHA256
verification. Authentication obtains a fresh `X-Auth-Token` for every operation,
as in the Go SDK. Temporary URLs and direct uploads are not used, and container
Temp-URL-Key metadata is not modified.

Unlike the Go SDK's default 180-day expiry, this backend omits `X-Delete-After`
so LFS objects do not expire automatically. Check that your container has no
external retention policy that removes objects. As with the S3 backend, deletes
are deliberately no-ops and object listing is not implemented.

### Production

To run in a production environment, it is easiest to use `docker-compose`:

 1. Create a `.env` file next to `docker-compose.yml` with the configuration
    variables:

    ```
    AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXQ
    AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    AWS_DEFAULT_REGION=us-west-1
    LFS_ENCRYPTION_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    LFS_S3_BUCKET=my-bucket
    LFS_MAX_CACHE_SIZE=10GB
    ```

 2. Use the provided `docker-compose.yml` file to run a production environment:

    ```bash
    docker-compose up -d

    # use minio yml
    docker-compose -f ./docker-compose.minio.yml up -d
    
    # use local disk yml
    docker-compose -f ./docker-compose.local.yml up -d
    ```

 3. **[Optional]**: It is best to use nginx as a reverse proxy for this server.
    Use it to enable TLS. How to configure this is better covered by other
    tutorials on the internet.

**Note**:
 - A bigger cache is (almost) always better. Try to use ~85% of the available
   disk space.
 - The cache data is stored in a Docker volume named `rudolfs_data`. If you
   want to delete it, run `docker volume rm rudolfs_data`.

## Request limits and storage safety

Organization and project names must use ASCII letters, digits, dots, underscores,
or hyphens; empty names and `.` or `..` are rejected. JSON request bodies are
limited to 1 MiB, batches to 1,000 objects, and backend lookups to 16 concurrent
operations per batch.

CDN mode cannot be combined with an encryption key because direct transfers
bypass server encryption. Origin authorization headers are only included in
server-handled actions, not CDN or presigned storage actions.

Failed and cancelled S3 uploads are aborted in the background. Configure an S3
`AbortIncompleteMultipartUpload` lifecycle rule as well, to clean up uploads
interrupted by process termination or failed abort requests.

## AWS Credentials

AWS credentials must be provided to the server so that it can make requests to
the S3 bucket specified on the command line (with `--bucket`).

Your AWS credentials will be searched for in the following order:

 1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
 2. AWS credentials file. Usually located at `~/.aws/credentials`.
 3. IAM instance profile. Will only work if running on an EC2 instance with an
    instance profile/role.

The AWS region is read from the `AWS_DEFAULT_REGION` or `AWS_REGION` environment
variable. If it is malformed, it will fall back to `us-east-1`. If it is not
present it will fall back on the value associated with the current profile in
`~/.aws/config` or the file specified by the `AWS_CONFIG_FILE` environment
variable. If that is malformed or absent it will fall back to `us-east-1`.

## Client Configuration

Add a file named `.lfsconfig` to the root of your Git repository and commit it
so everyone is using the same LFS server:

```
[lfs]
url = "http://gitlfs.example.com:8080/api/my-org/my-project"
              ─────────┬──────── ──┬─ ─┬─ ───┬── ─────┬────
                       │           │   │     │        └ Replace with your project's name
                       │           │   │     └ Replace with your organization name   
                       │           │   └ Required to be "api"
                       │           └ The port your server started with
                       └ The host name of your server
```

Optionally, I also recommend changing these global settings to speed things up:

``` bash
# Increase the number of worker threads
git config --global lfs.concurrenttransfers 64

# Use a global LFS cache to make re-cloning faster
git config --global lfs.storage ~/.cache/lfs
```

## License

[MIT License](/LICENSE)

## Thanks

This was developed at [Environmental Systems Research
Institute](http://www.esri.com/) (Esri) who have graciously allowed me to retain
the copyright and publish it as open source software.
