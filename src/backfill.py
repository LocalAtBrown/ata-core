"""
This needs to:
1. accept args
  a. site
  b. start date
  c. end date

2. fetch events from s3 (existing functionality)
3. process them (existing functionality)
4. store them (being written right now)

Notes:
- Might need to batch
- Run locally, or run on cloud? Start with local
"""

import click


@click.command()
def backfill():
    pass
