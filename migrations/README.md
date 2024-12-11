# Migrations

Go package that dumps tables from old databases (KDVH, Kvalobs) and imports them into LARD.

## Usage

1. Compile it with

   ```terminal
   go build
   ```

1. Dump tables

   ```terminal
   ./migrate kdvh dump
   ./migrate kvalobs dump
   ```

1. Import dumps into LARD

   ```terminal
   ./migrate kdvh import
   ./migrate kvalobs import
   ```

For each command, you can use the `--help` flag to see all available options.

## Other notes

Insightful talk on migrations: [here](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s)
