﻿//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Blob;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock
    {
        private string _lockFile;
        private AzureDirectory _azureDirectory;
        private string _leaseid;

        public AzureLock(string lockFile, AzureDirectory directory)
        {
            _lockFile = lockFile;
            _azureDirectory = directory;
        }

        #region Lock methods
        override public bool IsLocked()
        {
            var blob = _azureDirectory.BlobContainer.GetBlockBlobClient(_lockFile);
            try
            {
                Debug.Print("IsLocked() : {0}", _leaseid);
                if (string.IsNullOrEmpty(_leaseid))
                {
                    var leaseClient = blob.GetBlobLeaseClient(_leaseid);
                    var tempLease = leaseClient.Acquire(TimeSpan.FromSeconds(60)).Value;
                    if (string.IsNullOrEmpty(tempLease?.LeaseId))
                    {
                        Debug.Print("IsLocked() : TRUE");
                        return true;
                    }
                    leaseClient.Release();
                }
                Debug.Print("IsLocked() : {0}", _leaseid);
                return !string.IsNullOrEmpty(_leaseid);
            }
            catch (RequestFailedException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return IsLocked();
            }
            _leaseid = null;
            return false;
        }

        public override bool Obtain()
        {
            var blob = _azureDirectory.BlobContainer.GetBlockBlobClient(_lockFile);
            try
            {
                Debug.Print("AzureLock:Obtain({0}) : {1}", _lockFile, _leaseid);
                if (string.IsNullOrEmpty(_leaseid))
                {
                    _leaseid = blob.GetBlobLeaseClient(_leaseid).Acquire(TimeSpan.FromSeconds(60)).Value.LeaseId;
                    Debug.Print("AzureLock:Obtain({0}): AcquireLease : {1}", _lockFile, _leaseid);

                    // keep the lease alive by renewing every 30 seconds
                    long interval = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
                    _renewTimer = new Timer((obj) =>
                        {
                            try
                            {
                                AzureLock al = (AzureLock)obj;
                                al.Renew();
                            }
                            catch (Exception err) { Debug.Print(err.ToString()); }
                        }, this, interval, interval);
                }
                return !string.IsNullOrEmpty(_leaseid);
            }
            catch (RequestFailedException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return Obtain();
            }
            return false;
        }

        private Timer _renewTimer;

        public void Renew()
        {
            if (!string.IsNullOrEmpty(_leaseid))
            {
                Debug.Print("AzureLock:Renew({0} : {1}", _lockFile, _leaseid);
                var blob = _azureDirectory.BlobContainer.GetBlockBlobClient(_lockFile);
                blob.GetBlobLeaseClient(_leaseid).Renew();
            }
        }
        protected override void Dispose(bool disposing)
        {
            Release();
        }
        public /*override*/ void Release()
        {
            Debug.Print("AzureLock:Release({0}) {1}", _lockFile, _leaseid);
            if (!string.IsNullOrEmpty(_leaseid))
            {
                var blob = _azureDirectory.BlobContainer.GetBlockBlobClient(_lockFile);
                blob.GetBlobLeaseClient(_leaseid).Release();
                if (_renewTimer != null)
                {
                    _renewTimer.Dispose();
                    _renewTimer = null;
                }
                _leaseid = null;
            }
        }
        #endregion

        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", _lockFile, _leaseid);
            var blob = _azureDirectory.BlobContainer.GetBlockBlobClient(_lockFile);
            try
            {
                blob.GetBlobLeaseClient(_leaseid).Break();
            }
            catch (Exception)
            {
            }
            _leaseid = null;
        }

        public override string ToString() => string.Format("AzureLock@{0}.{1}", _lockFile, _leaseid);

        private bool _handleWebException(BlockBlobClient blob, RequestFailedException err)
        {
            if (err.Status == 404 || err.Status == 409)
            {
                _azureDirectory.CreateContainer();
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_lockFile);
                    try
                    {
                        blob.Upload(stream);
                    }
                    catch (Exception /*ex*/)
                    {
                        return false;
                    }

                }
                return true;
            }
            return false;
        }
    }
}
