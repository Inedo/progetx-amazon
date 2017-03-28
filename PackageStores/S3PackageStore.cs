using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Documentation;
using Inedo.IO;
using Inedo.ProGet.Extensibility.PackageStores;
using Inedo.ProGet.Web.Controls.Extensions;
using Inedo.Serialization;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Inedo.ProGet.Extensions.Amazon.PackageStores
{
    [DisplayName("Amazon S3")]
    [Description("A package store backed by Amazon S3.")]
    [CustomEditor(typeof(S3PackageStoreEditor))]
    [PersistFrom("Inedo.ProGet.Extensions.PackageStores.S3.S3PackageStore,ProGetCoreEx")]
    public sealed partial class S3PackageStore : CommonIndexedPackageStore
    {
        private readonly LazyDisposableAsync<AmazonS3Client> client;
        private bool disposed;

        public S3PackageStore()
        {
            this.client = new LazyDisposableAsync<AmazonS3Client>(this.CreateClient, this.CreateClientAsync);
        }

        [Persistent]
        public string AccessKey { get; set; }

        [Persistent]
        public string SecretAccessKey { get; set; }

        [Persistent]
        public string BucketName { get; set; }

        [Persistent]
        public string TargetPath { get; set; }

        [Persistent]
        public bool ReducedRedundancy { get; set; }

        [Persistent]
        public bool MakePublic { get; set; }

        [Persistent]
        public bool Encrypted { get; set; }

        [Persistent]
        public string RegionEndpoint { get; set; }

        private S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.NoACL;
        private S3StorageClass StorageClass => this.ReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;
        private ServerSideEncryptionMethod EncryptionMethod => this.Encrypted ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None;
        private string Prefix => string.IsNullOrEmpty(this.TargetPath) || this.TargetPath.EndsWith("/") ? this.TargetPath ?? string.Empty : (this.TargetPath + "/");

        public override RichDescription GetDescription()
        {
            var path = this.BucketName ?? string.Empty;
            if (!path.EndsWith("/"))
                path += "/";
            path += this.TargetPath?.Trim('/') ?? string.Empty;

            return new RichDescription(
                new Hilite(path),
                " on Amazon S3"
            );
        }

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.client.Dispose();
                }

                this.disposed = true;
            }

            base.Dispose(disposing);
        }

        protected override async Task<Stream> OpenReadAsync(string key)
        {
            try
            {
                var client = await this.client.ValueAsync.ConfigureAwait(false);
                var metadata = await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    }
                ).ConfigureAwait(false);

                return new BufferedStream(new DownloadStream(this, key, metadata.ContentLength), 64 * 1024);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }
        protected override Task<Stream> CreateAsync(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            return Task.FromResult<Stream>(new UploadStream(this, path));
        }
        protected override async Task DeleteAsync(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (this.disposed)
                throw new ObjectDisposedException(nameof(S3PackageStore));

            var client = await this.client.ValueAsync.ConfigureAwait(false);
            await client.DeleteObjectAsync(new DeleteObjectRequest { BucketName = this.BucketName, Key = path }).ConfigureAwait(false);
        }
        protected override async Task RenameAsync(string originalName, string newName)
        {
            if (string.IsNullOrEmpty(originalName))
                throw new ArgumentNullException(nameof(originalName));
            if (string.IsNullOrEmpty(newName))
                throw new ArgumentNullException(nameof(newName));

            if (string.Equals(originalName, newName, StringComparison.OrdinalIgnoreCase))
                return;

            var client = await this.client.ValueAsync.ConfigureAwait(false);

            await client.CopyObjectAsync(
                new CopyObjectRequest
                {
                    SourceBucket = this.BucketName,
                    DestinationBucket = this.BucketName,
                    SourceKey = originalName,
                    DestinationKey = newName,
                }
            ).ConfigureAwait(false);

            await this.DeleteAsync(originalName).ConfigureAwait(false);
        }
        protected override Task<IEnumerable<string>> EnumerateFilesAsync(string extension) => this.EnumerateFilesInternalAsync(string.Empty, extension);
        protected override string GetFullPackagePath(PackageStorePackageId packageId) => this.Prefix + this.GetRelativePackagePath(packageId, '/');

        private async Task<IEnumerable<string>> EnumerateFilesInternalAsync(string directory, string extension = null)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);

            var prefix = this.Prefix + directory;
            var files = Enumerable.Empty<S3Object>();
            var response = await client.ListObjectsAsync(
                new ListObjectsRequest
                {
                    BucketName = this.BucketName,
                    Prefix = prefix,
                }
            ).ConfigureAwait(false);

            while (true)
            {
                files = files.Concat(response.S3Objects);

                if (!response.IsTruncated)
                {
                    var names = files.Select(o => o.Key);
                    if (!string.IsNullOrEmpty(extension))
                        names = names.Where(n => n.EndsWith(extension, StringComparison.OrdinalIgnoreCase));
                    return names;
                }

                response = await client.ListObjectsAsync(
                    new ListObjectsRequest
                    {
                        BucketName = this.BucketName,
                        Prefix = prefix,
                        Marker = response.NextMarker,
                    }
                ).ConfigureAwait(false);
            }
        }

        private AmazonS3Client CreateClient() => new AmazonS3Client(this.AccessKey, this.SecretAccessKey, global::Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpoint));
        private Task<AmazonS3Client> CreateClientAsync() => Task.Run(() => this.CreateClient());

        private sealed class DownloadStream : Stream
        {
            private S3PackageStore store;
            private string key;

            public DownloadStream(S3PackageStore store, string key, long length)
            {
                this.store = store;
                this.key = key;
                this.Length = length;
            }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => false;

            public override long Length { get; }
            public override long Position { get; set; }

            public override void Flush()
            {
            }
            public override long Seek(long offset, SeekOrigin origin)
            {
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        this.Position = offset;
                        break;

                    case SeekOrigin.Current:
                        this.Position += offset;
                        break;

                    case SeekOrigin.End:
                        this.Position = this.Length + offset;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(origin));
                }

                return this.Position;
            }
            public override int Read(byte[] buffer, int offset, int count)
            {
                return this.ReadAsync(buffer, offset, count, CancellationToken.None).Result();
            }
            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                if (count == 0)
                    return 0;

                var client = await this.store.client.ValueAsync.ConfigureAwait(false);

                using (var obj = await client.GetObjectAsync(
                    new GetObjectRequest
                    {
                        BucketName = this.store.BucketName,
                        Key = this.key,
                        ByteRange = new ByteRange(this.Position, this.Position + count),
                    }
                ).ConfigureAwait(false))
                using (var remoteStream = obj.ResponseStream)
                {
                    int read = await remoteStream.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
                    this.Position += read;

                    return read;
                }
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }
        }

        private sealed class UploadStream : Stream
        {
            private const int MaxPutSize = 5 * 1024 * 1024;
            private S3PackageStore outer;
            private SlimMemoryStream currentPartStream = new SlimMemoryStream();
            private string key;
            private string uploadId;
            private bool disposed;
            private List<PartETag> parts = new List<PartETag>();

            public UploadStream(S3PackageStore outer, string key)
            {
                this.outer = outer;
                this.key = key;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;

            public override long Length
            {
                get { throw new NotSupportedException(); }
            }
            public override long Position
            {
                get { throw new NotSupportedException(); }
                set { throw new NotSupportedException(); }
            }

            public override void Flush()
            {
            }
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            protected override void Dispose(bool disposing)
            {
                if (!this.disposed)
                {
                    if (disposing)
                    {
                        this.FinalFlush();
                        this.currentPartStream.Dispose();
                    }

                    this.disposed = true;
                }

                base.Dispose(disposing);
            }

            public override void WriteByte(byte value)
            {
                if (this.disposed)
                    throw new ObjectDisposedException(nameof(UploadStream));

                int bufferBytesRemaining = MaxPutSize - (int)this.currentPartStream.Length;
                if (bufferBytesRemaining > 0)
                    this.currentPartStream.WriteByte(value);
                else
                    base.WriteByte(value);
            }
            public override void Write(byte[] buffer, int offset, int count)
            {
                this.WriteAsync(buffer, offset, count, CancellationToken.None).WaitAndUnwrapExceptions();
            }
            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                if (buffer == null)
                    throw new ArgumentNullException(nameof(buffer));
                if (offset < 0 || offset > buffer.Length)
                    throw new ArgumentOutOfRangeException(nameof(offset));
                if (count < 0)
                    throw new ArgumentOutOfRangeException(nameof(count));
                if (offset + count > buffer.Length)
                    throw new ArgumentException("The length of buffer is smaller than the sum of offset and count.");
                if (this.disposed)
                    throw new ObjectDisposedException(nameof(UploadStream));

                int bytesWritten = 0;
                while (bytesWritten < count)
                {
                    int bufferBytesRemaining = MaxPutSize - (int)this.currentPartStream.Length;
                    if (bufferBytesRemaining == 0)
                    {
                        await this.FlushMultipartBufferAsync(cancellationToken).ConfigureAwait(false);
                        bufferBytesRemaining = MaxPutSize;
                    }

                    int bytesToWriteToBuffer = Math.Min(bufferBytesRemaining, count - bytesWritten);
                    this.currentPartStream.Write(buffer, offset + bytesWritten, bytesToWriteToBuffer);
                    bytesWritten += bytesToWriteToBuffer;
                }
            }

            private async Task FlushMultipartBufferAsync(CancellationToken cancellationToken)
            {
                if (this.parts.Count == 0)
                {
                    var result = await this.outer.client.Value.InitiateMultipartUploadAsync(this.GetInitiateMultipartRequest(), cancellationToken).ConfigureAwait(false);
                    this.uploadId = result.UploadId;
                }

                this.currentPartStream.Position = 0;
                var uploadResult = await this.outer.client.Value.UploadPartAsync(this.GetUploadPartRequest(), cancellationToken).ConfigureAwait(false);
                this.parts.Add(new PartETag(uploadResult.PartNumber, uploadResult.ETag));

                this.currentPartStream.Position = 0;
                this.currentPartStream.SetLength(0);
            }
            private void FinalFlush()
            {
                this.currentPartStream.Position = 0;

                if (this.parts.Count == 0)
                {
                    this.outer.client.Value.PutObject(
                        new PutObjectRequest
                        {
                            BucketName = this.outer.BucketName,
                            AutoCloseStream = false,
                            InputStream = this.currentPartStream,
                            StorageClass = this.outer.StorageClass,
                            CannedACL = this.outer.CannedACL,
                            ServerSideEncryptionMethod = this.outer.EncryptionMethod,
                            Key = this.key
                        }
                    );
                }
                else
                {
                    if (this.currentPartStream.Length > 0)
                        this.FlushMultipartBufferAsync(CancellationToken.None).WaitAndUnwrapExceptions();

                    this.outer.client.Value.CompleteMultipartUpload(
                        new CompleteMultipartUploadRequest
                        {
                            BucketName = this.outer.BucketName,
                            Key = this.key,
                            PartETags = this.parts,
                            UploadId = this.uploadId
                        }
                    );
                }
            }

            private InitiateMultipartUploadRequest GetInitiateMultipartRequest()
            {
                return new InitiateMultipartUploadRequest
                {
                    BucketName = this.outer.BucketName,
                    StorageClass = this.outer.StorageClass,
                    CannedACL = this.outer.CannedACL,
                    ServerSideEncryptionMethod = this.outer.EncryptionMethod,
                    Key = this.key
                };
            }
            private UploadPartRequest GetUploadPartRequest()
            {
                return new UploadPartRequest
                {
                    BucketName = this.outer.BucketName,
                    Key = this.key,
                    UploadId = this.uploadId,
                    PartNumber = this.parts.Count + 1,
                    InputStream = this.currentPartStream
                };
            }
        }
    }
}
