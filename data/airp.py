import pathlib

import pandas

THIS_DIRECTORY = pathlib.Path(__file__).parent
df = pandas.read_csv(THIS_DIRECTORY / "airports.csv")
df.to_parquet('airports.parquet')
