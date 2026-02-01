"""Entry point: python -m oakd_viewer"""

import uvicorn

from .config import settings


def main():
    uvicorn.run(
        "oakd_viewer.app:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
