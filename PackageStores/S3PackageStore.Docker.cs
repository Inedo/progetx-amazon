using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Inedo.ProGet.Feeds.Docker;

namespace Inedo.ProGet.Extensions.Amazon.PackageStores
{
    partial class S3PackageStore : IDockerPackageStore
    {
        Task<Stream> IDockerPackageStore.OpenUploadAsync(string uploadId)
        {
            if (string.IsNullOrEmpty(uploadId))
                throw new ArgumentNullException(nameof(uploadId));

            return this.OpenReadAsync(this.GetDockerPath(uploadId));
        }
        Task<Stream> IDockerPackageStore.CreateUploadAsync(string uploadId)
        {
            if (string.IsNullOrEmpty(uploadId))
                throw new ArgumentNullException(nameof(uploadId));

            return this.CreateAsync(this.GetDockerPath(uploadId));
        }
        Task<Stream> IDockerPackageStore.ContinueUploadAsync(string uploadId)
        {
            // so far the docker client has never needed this...
            throw new NotImplementedException();
        }
        async Task<long> IDockerPackageStore.CompleteUploadAsync(string uploadId, DockerDigest digest)
        {
            if (string.IsNullOrEmpty(uploadId))
                throw new ArgumentNullException(nameof(uploadId));

            var sourceFileName = this.GetDockerPath(uploadId);

            if (digest != null)
            {
                var targetFileName = this.GetDockerPath("blobs", digest);

                await this.DeleteAsync(targetFileName).ConfigureAwait(false);
                await this.RenameAsync(sourceFileName, targetFileName).ConfigureAwait(false);

                return 1000;
            }
            else
            {
                await this.DeleteAsync(sourceFileName).ConfigureAwait(false);
                return 0;
            }
        }
        async Task<IEnumerable<string>> IDockerPackageStore.ListUploadsAsync()
        {
            return from p in await this.EnumerateFilesInternalAsync("uploads/").ConfigureAwait(false)
                   let u = this.GetUploadIdFromPath(p)
                   where u != null
                   select u;
        }

        Task<Stream> IDockerPackageStore.OpenBlobAsync(DockerDigest digest)
        {
            if (digest == null)
                throw new ArgumentNullException(nameof(digest));

            return this.OpenReadAsync(this.GetDockerPath("blobs", digest));
        }
        Task IDockerPackageStore.DeleteBlobAsync(DockerDigest digest)
        {
            if (digest == null)
                throw new ArgumentNullException(nameof(digest));

            return this.DeleteAsync(this.GetDockerPath("blobs", digest));
        }

        Task<Stream> IDockerPackageStore.OpenManifestAsync(DockerDigest digest)
        {
            if (digest == null)
                throw new ArgumentNullException(nameof(digest));

            return this.OpenReadAsync(this.GetDockerPath("manifests", digest));
        }
        Task<Stream> IDockerPackageStore.CreateManifestAsync(DockerDigest digest)
        {
            if (digest == null)
                throw new ArgumentNullException(nameof(digest));

            return this.CreateAsync(this.GetDockerPath("manifests", digest));
        }
        Task IDockerPackageStore.DeleteManifestAsync(DockerDigest digest)
        {
            if (digest == null)
                throw new ArgumentNullException(nameof(digest));

            return this.DeleteAsync(this.GetDockerPath("manifests", digest));
        }
        async Task<IEnumerable<DockerDigest>> IDockerPackageStore.ListManifestsAsync()
        {
            return from p in await this.EnumerateFilesInternalAsync("manifests/").ConfigureAwait(false)
                   let d = this.GetDigestFromPath("manifests", p)
                   where d != null
                   select d;
        }

        private string GetDockerPath(string uploadId) => this.Prefix + "uploads/" + uploadId;
        private string GetDockerPath(string type, DockerDigest digest) => $"{this.Prefix}{type}/{digest.Algorithm}/{digest.ToHashString()}";
        private string GetUploadIdFromPath(string path)
        {
            if (path == null || path.Length <= this.Prefix.Length)
                return null;

            var parts = path.Substring(this.Prefix.Length).Split('/');
            if (parts.Length != 2 || !string.Equals(parts[0], "uploads", StringComparison.OrdinalIgnoreCase))
                return null;

            return parts[1];
        }
        private DockerDigest GetDigestFromPath(string type, string path)
        {
            if (path == null || path.Length <= this.Prefix.Length)
                return null;

            var parts = path.Substring(this.Prefix.Length).Split('/');
            if (parts.Length != 3 || !string.Equals(parts[0], type, StringComparison.OrdinalIgnoreCase))
                return null;

            try
            {
                return DockerDigest.Parse(parts[1] + ":" + parts[2]);
            }
            catch
            {
                return null;
            }
        }
    }
}
