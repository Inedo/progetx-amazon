using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Inedo.Diagnostics;
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
using System.Text.RegularExpressions;
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
        // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html, sections "Characters That Might Require Special Handling" and "Characters to Avoid"
        private static readonly Regex UncleanPattern = new Regex(@"[&$\x00-\x1f\x7f@=;:+ ,?\\{^}%`\]""'>\[~<#|!]", RegexOptions.Compiled);

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

        private S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.AuthenticatedRead;
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
                var response = await client.GetObjectAsync(this.BucketName, key).ConfigureAwait(false);
                return new DownloadStream(response.ResponseStream, response);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.Forbidden && !this.MakePublic)
            {
                this.LogWarning($"Forbidden from downloading S3 object {this.BucketName}/{key}. Attempting to reset ACL.");
                var client = await this.client.ValueAsync.ConfigureAwait(false);

                try
                {
                    await client.PutACLAsync(new PutACLRequest
                    {
                        BucketName = this.BucketName,
                        Key = key,
                        CannedACL = this.CannedACL,
                    }).ConfigureAwait(false);
                }
                catch (AmazonS3Exception ex1)
                {
                    Exception hint;

                    try
                    {
                        hint = await this.GetPolicyHintAsync().ConfigureAwait(false);
                    }
                    catch (AmazonSecurityTokenServiceException ex2)
                    {
                        throw new AggregateException(ex, ex1, ex2);
                    }

                    throw new AggregateException("Due to a bug in a previous version of the Amazon package store, this package cannot be downloaded. Additionally, fixing the ACL failed.", hint, ex1, ex);
                }

                // Try again without catching exceptions.
                var response = await client.GetObjectAsync(this.BucketName, key).ConfigureAwait(false);
                return new DownloadStream(response.ResponseStream, response);
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
                    CannedACL = this.CannedACL,
                    ServerSideEncryptionMethod = this.EncryptionMethod,
                    StorageClass = this.StorageClass,
                }
            ).ConfigureAwait(false);

            await this.DeleteAsync(originalName).ConfigureAwait(false);
        }
        protected override Task<IEnumerable<string>> EnumerateFilesAsync(string extension) => this.EnumerateFilesInternalAsync(string.Empty, extension);
        protected override string GetFullPackagePath(PackageStorePackageId packageId) => this.Prefix + this.CleanPath(this.GetRelativePackagePath(packageId, '/'));

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
                    return names.ToList();
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

        private string CleanPath(string path)
        {
            // Replace the "dirty" ASCII characters with an exclamation mark followed by two hex digits.
            return UncleanPattern.Replace(path, m => "!" + ((byte)m.Value[0]).ToString("X2"));
        }

        private AmazonS3Client CreateClient() => new AmazonS3Client(this.AccessKey, this.SecretAccessKey, global::Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpoint));
        private Task<AmazonS3Client> CreateClientAsync() => Task.Run(() => this.CreateClient());

        private async Task<Exception> GetPolicyHintAsync()
        {
            using (var sts = new AmazonSecurityTokenServiceClient(this.AccessKey, this.SecretAccessKey, global::Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpoint)))
            {
                var identity = await sts.GetCallerIdentityAsync(new GetCallerIdentityRequest()).ConfigureAwait(false);

                var uniqueID = Guid.NewGuid().ToString("D");
                return new Exception($@"The following auto-generated policy may allow ProGet to upload and download packages. An account administrator can add the policy to the bucket at https://console.aws.amazon.com/s3/buckets/{this.BucketName}/?tab=permissions

{{
  ""Id"": ""ProGet{uniqueID}"",
  ""Version"": ""2012-10-17"",
  ""Statement"": [
    {{
      ""Sid"": ""ProGet{uniqueID}"",
      ""Action"": [
        ""s3:GetObject"",
        ""s3:PutObject"",
        ""s3:PutObjectAcl"",
        ""s3:InitiateMultipartUpload"",
        ""s3:UploadPart"",
        ""s3:CompleteMultipartUpload"",
        ""s3:ListObjects"",
        ""s3:DeleteObject""
      ],
      ""Effect"": ""Allow"",
      ""Resource"": ""arn:aws:s3:::{this.BucketName}/{this.Prefix}*"",
      ""Principal"": {{
        ""AWS"": [
          ""{identity.Arn}""
        ]
      }}
    }}
  ]
}}");
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
                try
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
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.Forbidden)
                {
                    var hint = await this.outer.GetPolicyHintAsync().ConfigureAwait(false);
                    throw new AggregateException("ProGet received an Access Denied response from Amazon S3. This may be due to access policies. A sample policy is listed with this exception in Recent Errors.", hint, ex);
                }
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
        private sealed class DownloadStream : Stream
        {
            private Stream stream;
            private GetObjectResponse response;
            private Stream temp;
            private AutoResetEvent cond;
            private SemaphoreSlim mutex;
            private CancellationTokenSource cancellationSource;
            private Task task;
            private long readPosition;
            private long writePosition;
            private bool disposed;

            public DownloadStream(Stream stream, GetObjectResponse response)
            {
                this.stream = stream;
                this.response = response;
                this.temp = TemporaryStream.Create(response.ContentLength);
                this.cond = new AutoResetEvent(false);
                this.mutex = new SemaphoreSlim(1, 1);
                this.cancellationSource = new CancellationTokenSource();
                this.task = this.CopyFromStreamAsync(this.cancellationSource.Token);
            }

            private async Task CopyFromStreamAsync(CancellationToken cancellationToken)
            {
                var buffer = new byte[4096];
                while (true)
                {
                    var count = await this.stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                    if (count == 0)
                    {
                        return;
                    }

                    await this.mutex.WaitAsync(cancellationToken);
                    try
                    {
                        await this.temp.WriteAsync(buffer, 0, count, cancellationToken);
                        this.writePosition += count;
                    }
                    finally
                    {
                        this.mutex.Release();
                    }

                    this.cond.Set();
                }
            }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => false;
            public override long Length => this.response.ContentLength;
            public override long Position
            {
                get
                {
                    this.mutex.Wait();
                    try
                    {
                        return this.readPosition;
                    }
                    finally
                    {
                        this.mutex.Release();
                    }
                }
                set
                {
                    this.mutex.Wait();
                    try
                    {
                        if (value < 0 || this.response.ContentLength < value)
                        {
                            throw new ArgumentOutOfRangeException(nameof(Position));
                        }
                        this.readPosition = value;
                    }
                    finally
                    {
                        this.mutex.Release();
                    }
                }
            }

            public override void Flush() { }

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (count == 0)
                {
                    return 0;
                }

                while (true)
                {
                    this.mutex.Wait();
                    try
                    {
                        if (this.readPosition >= this.response.ContentLength)
                        {
                            return 0;
                        }

                        if (this.writePosition > this.readPosition)
                        {
                            this.temp.Position = this.readPosition;
                            int n = this.temp.Read(buffer, offset, count);
                            this.readPosition += n;
                            return n;
                        }
                    }
                    finally
                    {
                        this.mutex.Release();
                    }

                    this.cond.WaitOne();
                }
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                if (count == 0)
                {
                    return 0;
                }

                while (true)
                {
                    await this.mutex.WaitAsync(cancellationToken);
                    try
                    {
                        if (this.readPosition >= this.response.ContentLength)
                        {
                            return 0;
                        }

                        if (this.writePosition > this.readPosition)
                        {
                            this.temp.Position = this.readPosition;
                            int n = await this.temp.ReadAsync(buffer, offset, count, cancellationToken);
                            this.readPosition += n;
                            return n;
                        }
                    }
                    finally
                    {
                        this.mutex.Release();
                    }

                    await Task.Factory.StartNew(this.cond.WaitOne, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Current);
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        break;
                    case SeekOrigin.Current:
                        offset += this.Position;
                        break;
                    case SeekOrigin.End:
                        offset += this.response.ContentLength;
                        break;
                }
                return this.Position = offset;
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            protected override void Dispose(bool disposing)
            {
                if (!this.disposed)
                {
                    this.disposed = true;
                    this.cancellationSource.Cancel();
                    try
                    {
                        this.task.WaitAndUnwrapExceptions();
                    }
                    catch (TaskCanceledException)
                    {
                    }
                    this.cancellationSource.Dispose();
                    this.stream.Dispose();
                    this.response.Dispose();
                    this.cond.Dispose();
                    this.mutex.Dispose();
                    this.temp.Dispose();
                }
                base.Dispose(disposing);
            }
        }
    }
}
