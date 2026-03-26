# adapted from https://github.com/RDFLib/rdflib/discussions/1560

from collections.abc import Callable
from io import TextIOWrapper
from queue import Queue
import threading
import time
from types import FunctionType
from typing import Generator, TypeVar
from rdflib.graph import Graph
from rdflib import Literal, URIRef
from rdflib.term import Node
import gzip
import bz2
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path


def parse_nt_to_generator(
    source: str | Path, buffer=10
) -> Generator[tuple[Node, Node, Node]]:
    q = Queue(maxsize=buffer)
    source = Path(source)
    assert (
        source.name.endswith(".nt")
        or source.name.endswith(".nt.gz")
        or source.name.endswith(".nt.bz2")
    ), "Source file must be a .nt, .nt.gz, or .nt.bz2 file"

    class TripleStream:
        def parse_line(self, line: str):
            try:
                g = Graph()
                g.parse(data=line, format="nt")
                for triple in g:
                    q.put(triple)
            except Exception as e:
                q.put(e)

        def parse(self, source: TextIOWrapper, format="nt"):
            for line in source:
                self.parse_line(line)

    def task():
        if source.name.endswith(".gz"):
            with gzip.open(source, "rt") as f:
                # skip the header
                f.readline()

                g = TripleStream()
                g.parse(f)
        elif source.name.endswith(".bz2"):
            with bz2.open(source, "rt") as f:
                # skip the header
                f.readline()
                g = TripleStream()
                g.parse(f)
        else:
            with open(source, "r") as f:
                g = TripleStream()
                g.parse(f)
        q.put(None)

    t = threading.Thread(target=task)
    t.start()

    while True:
        triple = q.get(timeout=5)
        if not triple:
            break
        if type(triple) is Exception:
            raise Exception
        yield triple

    t.join()


def save_from_generator(
    destination: str | Path, generator: Generator[tuple[Node, Node, Node]]
):
    destination = Path(destination)
    counter = 0
    if destination.name.endswith(".gz"):
        with gzip.open(destination, "wt") as f:
            for triple in generator:
                counter += 1
                output_rdf_triple(f, triple)
    elif destination.name.endswith(".bz2"):
        with bz2.open(destination, "wt") as f:
            for triple in generator:
                counter += 1
                output_rdf_triple(f, triple)
    else:
        with open(destination, "w") as f:
            for triple in generator:
                counter += 1
                output_rdf_triple(f, triple)
    return counter


def output_rdf_triple(fd: TextIOWrapper, triple: tuple[Node, Node, Node]):
    s, p, o = triple
    line = f"{s.n3()} {p.n3()} {o.n3()} .\n"
    fd.write(line)
