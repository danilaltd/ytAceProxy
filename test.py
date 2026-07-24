from yt_dlp import YoutubeDL, parse_options
import pprint
pp = pprint.PrettyPrinter(indent=4)

def python_yt_dlp_get_link(url: str) -> str:
    with YoutubeDL(parse_options(["--quiet", "--skip-download", "--format", "best[acodec!=none]", url]).ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if "requested_formats" in info:
        raise Exception(f"expected 1 link, got multiple: {info}")
    res = info.get("url")
    if isinstance(res, str):
        print(res)
        # with open('output.py', 'w') as output_file:
        #     formatted_data  = pp.pformat(info)
        #     output_file.write(f"{formatted_data} # type: ignore\n")
        return res
    raise Exception(f"yt-dlp returned non-string url: {info}")

python_yt_dlp_get_link("https://www.youtube.com/watch?v=oznr-1-poSU")