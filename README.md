# bits
store byte streams as deduplicated, encrypted and content-addressable chunks in a memory-mapped database

## Basic Usage

split, encrypt and store a large file de-duplicated on your system, writing the
key index (of the file) to a plain text file:

```
cat ./large-file.bin | bits split > large-file-keys.txt
```

decrypt and re-combine a large file on your system. Will lazily fetch
chunks missing locally from the remote.

```
cat ./large-file-keys.txt | bits join -r=https://my-bucket.s3.amazonaws.com > ./large-file.bin
```

push the encrypted chunks of a file to a remote storage. Indexes the remote
first and skips and chunks that already present.

```
cat ./large-file-keys.txt | bits push -r=https://my-bucket.s3.amazonaws.com
```

fetch the encrypted chunks of a file from a remote storage into the local storage
without decrypting

```
cat ./large-file-keys.txt | bits fetch -r=https://my-bucket.s3.amazonaws.com
```
