# Migrations

Go package used to dump tables from old databases (KDVH, Kvalobs) and import them into LARD.

## Usage

1. Compile it with

   ```terminal
   go build
   ```

1. Dump tables from KDVH

   ```terminal
   ./migrate kdvh dump
   ```

1. Import dumps into LARD

   ```terminal
   ./migrate kdvh import
   ```

For each command, you can use the `--help` flag to see all available options.

## Other notes

Insightful talk on migrations: [here](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s)
