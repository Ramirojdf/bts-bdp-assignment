from os.path import dirname, join

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))


class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default="bdi-test",
        description="Call the api like `BDI_S3_BUCKET=yourbucket uvicorn ...`",
    )

    model_config = SettingsConfigDict(env_prefix="bdi_")

    @property
    def raw_dir(self) -> str:
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")
