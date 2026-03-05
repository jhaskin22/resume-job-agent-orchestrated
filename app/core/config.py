from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Scaffolded App"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")


settings = Settings()
