using System.ComponentModel;
using Inedo.ProGet.Extensibility.FileSystems;
using Inedo.ProGet.Extensibility.PackageStores;
using Inedo.ProGet.Web.Controls.Extensions;
using Inedo.Serialization;

namespace Inedo.ProGet.Extensions.Amazon.PackageStores
{
    [DisplayName("Amazon S3")]
    [Description("A package store backed by Amazon S3.")]
    [CustomEditor(typeof(S3PackageStoreEditor))]
    [PersistFrom("Inedo.ProGet.Extensions.PackageStores.S3.S3PackageStore,ProGetCoreEx")]
    public sealed class S3PackageStore : FileSystemPackageStore
    {
        private readonly S3FileSystem fileSystem = new S3FileSystem();
        public override FileSystem FileSystem => fileSystem;

        [Persistent]
        public string AccessKey
        {
            get => this.fileSystem.AccessKey;
            set => this.fileSystem.AccessKey = value;
        }

        [Persistent]
        public string SecretAccessKey
        {
            get => this.fileSystem.SecretAccessKey;
            set => this.fileSystem.SecretAccessKey = value;
        }

        [Persistent]
        public string BucketName
        {
            get => this.fileSystem.BucketName;
            set => this.fileSystem.BucketName = value;
        }

        [Persistent]
        public string TargetPath
        {
            get => this.fileSystem.TargetPath;
            set => this.fileSystem.TargetPath = value;
        }

        [Persistent]
        public bool ReducedRedundancy
        {
            get => this.fileSystem.ReducedRedundancy;
            set => this.fileSystem.ReducedRedundancy = value;
        }

        [Persistent]
        public bool MakePublic
        {
            get => this.fileSystem.MakePublic;
            set => this.fileSystem.MakePublic = value;
        }

        [Persistent]
        public bool Encrypted
        {
            get => this.fileSystem.Encrypted;
            set => this.fileSystem.Encrypted = value;
        }

        [Persistent]
        public string RegionEndpoint
        {
            get => this.fileSystem.RegionEndpoint;
            set => this.fileSystem.RegionEndpoint = value;
        }
    }
}
