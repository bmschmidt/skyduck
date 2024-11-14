import typer
import pyarrow as pa
import pyarrow.json as paj
from pathlib import Path


print("HI")
app = typer.Typer()


@app.command()
def main():
  print("YO")
  for p in Path("collections").glob("*.jsonl"):
    if p.stat().st_size < 10_000:
      continue
    print("===========")
    print(p)
    print("============")
    r = paj.read_json(p)
    print(r.schema)

if __name__ == "__main__":
  app()