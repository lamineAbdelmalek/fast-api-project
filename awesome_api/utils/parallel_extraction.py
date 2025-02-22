from multiprocessing import Pool
from typing import Callable, Iterable, Sequence

import pandas as pd
from pandas import DataFrame


def values_chunker(
    values: Sequence[str],
    chunk_size: int,
) -> Iterable[Sequence[str]]:
    n_chunks = len(values) // chunk_size
    if (n_chunks % chunk_size) > 0:
        n_chunks += 1
    for i in range(n_chunks):
        chunk = values[i * chunk_size : (i + 1) * chunk_size]  # noqa E203
        yield chunk


def parallel_execution(
    func: Callable[..., DataFrame],
    values: Sequence[str],
    chunk_size: int,
    pool_size: int,
) -> DataFrame:
    with Pool(pool_size) as p:
        df_list = p.map(func, values_chunker(values=values, chunk_size=chunk_size))
    df = pd.concat(df_list).reset_index(drop=True)
    return df


def dummy_func(chunk: Sequence[str]) -> DataFrame:
    return pd.DataFrame({"a": chunk})


def multiple_dummy_func(
    values: Sequence[str], chunk_size: int, pool_size: int
) -> DataFrame:
    return parallel_execution(
        func=dummy_func, values=values, chunk_size=chunk_size, pool_size=pool_size
    )


if __name__ == "__main__":
    print(
        multiple_dummy_func(
            values=[f"a_{i}" for i in range(1, 51256)], chunk_size=1000, pool_size=6
        ).tail(10)
    )
