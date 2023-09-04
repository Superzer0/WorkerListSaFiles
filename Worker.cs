using System.Diagnostics;
using System.Text;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;

namespace WorkerListSaFiles
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var saName = _configuration["saName"] ?? throw new Exception("sa name is null");
                var containerName = _configuration["containerName"] ?? throw new Exception("container name is null");

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                _logger.LogInformation("Connecting to : {saName}/{container}", saName, containerName);

                var blobClient = GetBlobServiceClient(saName);
                var containerClient = blobClient.GetBlobContainerClient(containerName);

                await ListBlobsFlatListing(containerClient, 100);
                await Task.Delay(2000, stoppingToken);
            }
        }


        public BlobServiceClient GetBlobServiceClient(string accountName)
        {
            BlobServiceClient client = new(
                new Uri($"https://{accountName}.blob.core.windows.net"),
                new DefaultAzureCredential());

            return client;
        }

        private async Task ListBlobsFlatListing(BlobContainerClient blobContainerClient,
            int? segmentSize)
        {
            try
            {
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                // Call the listing operation and return pages of the specified size.
                var resultSegment = blobContainerClient.GetBlobsAsync()
                    .AsPages(default, segmentSize);
                var stringBuilder = new StringBuilder();

                // Enumerate the blobs returned for each page.
                await foreach (Page<BlobItem> blobPage in resultSegment)
                {
                    foreach (var blobItem in blobPage.Values)
                    {
                        stringBuilder.AppendLine($"Blob name: {blobItem.Name}");
                    }
                }

                _logger.LogInformation("{blobContents}", stringBuilder.ToString());
                _logger.LogInformation("Took {timespan}", stopWatch.Elapsed);
            }
            catch (RequestFailedException e)
            {
                _logger.LogError(e, "Error when connecting to storage account");
                throw;
            }
        }
    }
}