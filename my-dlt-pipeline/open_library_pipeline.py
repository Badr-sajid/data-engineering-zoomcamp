"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source(bibkeys: str = "ISBN:0451526538"):
    """Define dlt resources from Open Library REST API.

    Starts with the `books` endpoint (`/api/books`) and requests JSON
    data. The `bibkeys` parameter can be used to provide keys such as
    `ISBN:0451526538,OLID:OL123M`.
    """
    config: RESTAPIConfig = {
        "client": {
            # base Open Library URL
            "base_url": "https://openlibrary.org",
            # no auth required for the public books API
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    # endpoint documented at https://openlibrary.org/dev/docs/api/books
                    "path": "/api/books",
                    # default params: format=json and jscmd=data to get expanded info
                    "params": {"format": "json", "jscmd": "data", "bibkeys": bibkeys},
                    # extract all values from the root object (bibkeys are keys, so we get all book objects)
                    "data_selector": "$[*]",
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
