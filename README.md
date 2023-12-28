# MailHedgehog storage implementation for shared messaged rows, by storing in csv file.

Stores all shared messages data in csv file.

## Usage

```go
storage := messageSharingStorageFileCsv.CreateSharingEmailUsingCSV(&messageSharingStorageFileCsv.StorageConfiguration{Path: '.mh-sharing.csv'})

storage.DeleteExpired()
```

## Development

```shell
go mod tidy
go mod verify
go mod vendor
go test --cover
```

## Credits

- [![Think Studio](https://yaroslawww.github.io/images/sponsors/packages/logo-think-studio.png)](https://think.studio/)
