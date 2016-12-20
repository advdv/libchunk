# bits
Store arbitrary sized byte streams as de-duplicated, encrypted and content-addressable
chunks in a memory-mapped database and push them to an S3 object store for persistence.

## Basic Usage

Split, encrypt and store a large file de-duplicated on your system, writing the
key list (of the file) to a plain text file:

```
cat ./large-file.bin | bits split -l=~/bits.db > large-file-keys.txt
```

Push the encrypted chunks of a file to an S3 object store. When possible, index
the remote first to enable chunks to be skipped that are already present.

```
cat ./large-file-keys.txt | bits push -l=~/bits.db -r=https://my-bucket.s3.amazonaws.com
```

Decrypt and re-combine a large file on your system. Will lazily fetch
chunks from the remote when they are missing locally.

```
cat ./large-file-keys.txt | bits join -l=~/bits.db -r=https://my-bucket.s3.amazonaws.com > ./large-file.bin
```
