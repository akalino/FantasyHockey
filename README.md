# FantasyHockey

Some Python scripts to scrape info from various hockey statistics providers.
There are also files for creating the Postgres schema that this data is stored in as
well as scripts that are used to perform the ETL on the fetched data that are cronned to
run on a nightly basis. Queries over this data are used to inform lineup decisions
for the 2016-2017 Smash'em Check'em fantasy hockey league.