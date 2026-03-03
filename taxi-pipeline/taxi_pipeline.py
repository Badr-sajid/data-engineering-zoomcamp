"""Pipeline to ingest NYC taxi data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from NYC taxi data REST API endpoints."""
    # Create a custom resource that handles pagination manually
    @dlt.resource(name="trips")
    def get_trips():
        import requests
        
        base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
        page = 1
        
        while True:
            # Fetch a page
            response = requests.get(f"{base_url}?limit=1000&page={page}")
            data = response.json()
            
            # Stop when empty page
            if not data:
                break
            
            # Yield records
            for record in data:
                yield record
            
            page += 1
            print(f"Loaded page {page-1}", flush=True)
    
    return get_trips()


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
