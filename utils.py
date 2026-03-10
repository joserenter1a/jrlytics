import pathlib

THIS_DIRECTORY = pathlib.Path(__file__).parent


def fread(path: pathlib.Path) -> str:
    """Read a file and return its contents as a string.

    Args:
        path: Absolute path to the file to read.

    Returns:
        The full contents of the file as a string.

    Raises:
        FileNotFoundError: If no file exists at *path*.
    """
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        raise
    except Exception:
        raise
