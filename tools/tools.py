"""
Collection of useful functions and classes.
"""

import codecs
from contextlib import contextmanager
from datetime import timedelta
import enum
import os
import subprocess
import sys
import time
import urllib.request
from tqdm import tqdm


class MessageType(enum.Enum):
    """
    Specifies the type of output messages.

    Parameters
    ----------
    enum : enum.Enum
        base enum class
    """
    INFO = 0
    WARNING = 1
    ERROR = 2

    def __str__(self):
        return self.name
    
    

class DownloadProgressBar(tqdm):
    """
    Used to show download progress bar.
    Copied from https://stackoverflow.com/questions/15644964/python-progress-bar-and-downloads
    """
    def update_to(self, blocks=1, block_size=1, total_size=None):
        """
        Manually updates the progress bar to the specified position

        Parameters
        ----------
        blocks : int, optional
            number of blocks, by default 1
        block_size : int, optional
            size of blocks, by default 1
        tsize : int, optional
            the expected total number of operations, by default None
        """
        if total_size is not None:
            self.total = total_size
        self.update(blocks * block_size - self.n)


COLORS_ENABLED = False
VERBOSE = False
WARNING_MESSAGES = []
ERROR_MESSAGES = []
EXCEPTION_MESSAGES = []


def _get_message_prefix(message_type: MessageType):
    """ Generates prefix for info messages,
        containing the message type and the current time.

    Parameters
    ----------
    message_type : MessageType
        Type of the message: info, warning or error

    Returns
    -------
    [str]
        Message prefix, possibly with a color code
    """
    result = f"[{message_type}--{time_now()}]"
    if COLORS_ENABLED:
        color_code = ""
        if message_type == MessageType.INFO:
            color_code = "34"
        elif message_type == MessageType.WARNING:
            color_code = "93"
        elif message_type == MessageType.ERROR:
            color_code = "31"
        assert color_code != ""
        result = f"\033[{color_code}m{result}\033[0m"
        
    return result


def print_info_message(message: str, level: int = 1):
    """
    Prints an informational message in a defined format.

    Parameters
    ----------
    message : str
        message to print
    level : int, optional
        message indent level, by default 1 (0 = title, 1 or above = regular messages)
    """
    indent_level = "===" if level == 0 else level * "--"
    formatted_message = f"{indent_level}> {message}"
    if level == 0:
        formatted_message = formatted_message.upper()
    print(f"{_get_message_prefix(MessageType.INFO)} {formatted_message}", flush=True)


def print_warning_message(message: str):
    """
    Prints a warning message in a defined format.

    Parameters
    ----------
    message : str
        message to print
    """
    global WARNING_MESSAGES  # pylint: disable=global-variable-not-assigned
    WARNING_MESSAGES.append(message)
    print(f"{_get_message_prefix(MessageType.WARNING)} {message}  -- [TOTAL WARNINGS]: {len(WARNING_MESSAGES)}", flush=True)


def print_error_message(message: str):
    """
    Prints an error message in a defined format.

    Parameters
    ----------
    message : str
        message to print
    """
    global ERROR_MESSAGES  # pylint: disable=global-variable-not-assigned
    ERROR_MESSAGES.append(message)
    print(f"{_get_message_prefix(MessageType.ERROR)} {message}  -- [TOTAL ERRORS]: {len(ERROR_MESSAGES)}", flush=True)


def print_exception_message(message: str, print_full_traceback = False):
    """
    Prints an exception message in a defined format.

    Parameters
    ----------
    message : str
        message to print
    print_full_traceback : bool, optional
        full traceback information, by default False
    """
    global EXCEPTION_MESSAGES  # pylint: disable=global-variable-not-assigned
    EXCEPTION_MESSAGES.append(message)
    exc_type, exc_obj, exc_tb = sys.exc_info()  # pylint: disable=unused-variable
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    if print_full_traceback:
        if exc_tb is not None:
            prev = exc_tb
            curr = exc_tb.tb_next
            while curr is not None:
                prev = curr
                curr = curr.tb_next
            print(prev.tb_frame.f_locals)
    print(exc_type, fname, exc_tb.tb_lineno)
    print(f"{_get_message_prefix(MessageType.ERROR)} {message} -- [TOTAL EXCEPTIONS]: {len(EXCEPTION_MESSAGES)}", flush=True)
    if VERBOSE and len(EXCEPTION_MESSAGES) % 10 == 0:
        print("EXCEPTION MESSAGES SO FAR:")
        for message in EXCEPTION_MESSAGES:
            print(message)


def print_final_statistics(start_time: float, end_time: float):
    print_info_message("===== EXECUTION STATS ==========", 2)
    print_info_message(f"Warnings       : {len(WARNING_MESSAGES):,}", 2)
    print_info_message(f"Errors         : {len(ERROR_MESSAGES):,}", 2)
    print_info_message(f"Exceptions     : {len(EXCEPTION_MESSAGES):,}", 2)
    print_info_message(f"Execution time : {str(timedelta(seconds=round(end_time - start_time)))}", 2)
    print_info_message("================================", 2)
    print_info_message(f"Done.", 0)


# taken from https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
def timeit(method):
    """
    Computes the execution time of the provided method.

    Parameters
    ----------
    method : classmethod
        method for which the execution time has to be calculated
    """
    def timed(*args, **kw):
        time_start = time.time()
        result = method(*args, **kw)
        time_end = time.time()
        
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((time_end - time_start) * 1000)
        else:
            print(f"{method.__name__} {(time_end - time_start) * 1000:.2f} ms")
        return result
    return timed


def time_now():
    """
    Provides the current time.

    Returns
    -------
    String
        current time
    """
    return time.strftime("%H:%M:%S", time.localtime())


def download_url(url: str, file_name: str, verbose: bool = False):
    """
    Downloads the file at the provided URL.

    Parameters
    ----------
    url : str
        URL of the file
    file_name : str
        name of the output file
    verbose : bool, optional
        indicates whether more messages should be displayed, by default False
    """
    if verbose:
        print_info_message(f"Downloading URL '{url}'...")
    with DownloadProgressBar(unit='B', unit_scale=True, miniters=1, desc=url.split('/')[-1]) as progress_bar:
        urllib.request.urlretrieve(url, filename=file_name, reporthook=progress_bar.update_to)
    if verbose:
        print_info_message(f"File stored in '{file_name}'.", 2)


def download_and_extract_gz_file(url: str, file_name: str, verbose: bool = False):
    """
    Downloads a gzipped file and extracts it to the provided location.

    Parameters
    ----------
    url : str
        URL of the gzipped file to download
    file_name : str
        location where the extracted file will be stored
    """
    gzipped_file_name = file_name + ".gz"
    download_url(url, gzipped_file_name, verbose)
    if verbose:
        print_info_message(f"Gunzipping {gzipped_file_name}...")
    args = f"gunzip -f {gzipped_file_name}".split()
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    if verbose:
        print_info_message(f"File stored in '{file_name}'.", 2)
        
        
def download_and_extract_targz_folder(url: str, folder_name: str, verbose: bool = False):
    """
    Downloads a tar-gzipped folder and extracts it to the provided location.

    Parameters
    ----------
    url : str
        URL of the gzipped file to download
    folder_name : str
        location where the extracted folder will be stored
    """
    args = f"rm -rf {folder_name}".split()
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    
    args = f"mkdir {folder_name}".split()
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    
    gzipped_file_name = folder_name + ".tar.gz"
    download_url(url, gzipped_file_name, verbose)
    if verbose:
        print_info_message(f"Extracting files from '{gzipped_file_name}'...")

    args = f"tar -C {folder_name} -xzvf {gzipped_file_name}".split()
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    if verbose:
        print_info_message(f"Extracted files stored in '{folder_name}'.", 2)


def convert_file_encoding(file_name: str, original_encoding: str, target_encoding: str):
    """
    Converts the file with the provided file name from the original encoding
    to the target encoding.

    Parameters
    ----------
    file_name : str
        name of the file to convert
    original_encoding : str
        original encoding
    target_encoding : str
        target encoding
    """
    with codecs.open(file_name, 'r', encoding = original_encoding) as file:
        lines = file.read()  
    with codecs.open(file_name, 'w', encoding = target_encoding, errors="ignore") as file:
        file.write(lines)


def extract_tar_file(tar_file_name: str, verbose: bool = False):
    """
    Extracts the provided tar file.

    Parameters
    ----------
    tar_file_name : str
        location of the file to extract
    """
    if verbose:
        print_info_message(f"Extracting {tar_file_name}...")
    if '/' in tar_file_name:  # then extract the folder and the file name
        folder_name = tar_file_name[:tar_file_name.rfind('/')]
        if verbose:
            print_info_message(f"File name: '{tar_file_name}', folder: '{folder_name}'")
        args = f"tar -C {folder_name} -xvf {tar_file_name}".split()
    else:
        args = f"tar -xvf {tar_file_name}".split()
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    if verbose:
        print_info_message("Done.")


@contextmanager
def suppress_output():
    """ Temporary removes everything written on the console
    """
    with open(os.devnull, "w", encoding="utf-8") as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = devnull
        sys.stderr = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
