import os
import csv
import json
import pydoi
import requests
from tqdm import tqdm
from os.path import exists
from collections import defaultdict
import argparse
import subprocess


def get_references(query):
    fields = "title,year,externalIds,venue,openAccessPdf"
    # headers = {'x-api-key': ''}
    url = f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&year=2000-&venue=Interspeech&limit=20000"
    r = requests.get(url).json()
    print(f"Will retrieve an estimated {r['total']} documents")

    retrieved = 0
    with open("papers.jsonl", "a") as file:
        while True:
            if "data" in r:
                retrieved += len(r["data"])
                print(f"Retrieved {retrieved} papers...")
                for paper in r["data"]:
                    print(json.dumps(paper), file=file)
            if "token" not in r:
                break
            r = requests.get(f"{url}&token={r['token']}").json()

    print(f"Done! Retrieved {retrieved} papers total")

def extract_dois(in_f, out_f):
    with open(in_f, "r", encoding="utf-8") as fp, open(
        f"{out_f}.txt", "w", encoding="utf-8"
    ) as outf, open(f"{out_f}.err", "w", encoding="utf-8") as err:
        dois = []
        for line in tqdm(fp):
            line = json.loads(line)
            try:
                idx = line["externalIds"]["DOI"]
                title = line["title"]
                year = line["year"]
                outf.write(f"{idx},{title},{year}\n")
                dois.append(idx)
            except Exception:
                idx = line["externalIds"]["CorpusId"]
                title = line["title"]
                year = line["year"]
                print(json.dumps(line), file=err)
    return dois

def parse_doi_list(doi_txt):
    parsed_dois = []
    for line in doi_txt:
        try:
            doi, title, year = [part.strip() for part in line.split(",", 2)]
            parsed_dois.append(doi)
        except ValueError:
            print(f"Skipping line: {line}")
    return parsed_dois

def extract_urls(doi_txt, out_f):
    doi_list = parse_doi_list(doi_txt)
    with open(f"{out_f}.txt", "w", encoding="utf-8") as outf, open(
        f"{out_f}.err", "w", encoding="utf-8"
    ) as err:
        err_count = 0
        suc_count = 0

        for doi in tqdm(doi_list):
            resp = pydoi.get_url(doi)
            if resp:
                suc_count += 1
                outf.write(f"{doi},{resp}\n")
            else:
                err.write(f"{doi}\n")
                err_count += 1

        print(f"Success: {suc_count}, Errors: {err_count}")

def parse_url(url):
    # Parse every URL and change to a PDF
    if "arxiv" in url:
        url = url.replace("abs", "pdf")
        url = f"{url}.pdf"
    elif "isca-archive" in url:
        url = url.replace("html", "pdf")
    return url

def download_pdf(url, out_dir="papers"):
    os.makedirs(out_dir, exist_ok=True)
    filename = url.split("/")[-1]
    path = os.path.join(out_dir, filename)
    resp = requests.get(url)
    with open(path, "wb") as f:
        f.write(resp.content)
    return path

def download_pdfs(in_file):
    with open(in_file, "r", encoding="utf-8") as f:
        for line in tqdm(f):
            try:
                doi, url = line.strip().split(",")
                parsed = parse_url(url)
                download_pdf(parsed)
            except Exception as e:
                with open("download.err", "a") as err:
                    err.write(f"{doi},{url},{e}\n")

def extract_texts(pdf_dir="papers", text_dir="papers/pdf_texts"):
    # was having issues with adding poppler to my PATH - specify the path to pdftotext.exe for workaround
    # PDFTOTEXT_EXE = r"your-path-here"
    os.makedirs(text_dir, exist_ok=True)
    error_log_path = "extract_texts.err"
    for filename in tqdm(os.listdir(pdf_dir)):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(pdf_dir, filename)
            txt_path = os.path.join(text_dir, filename.replace(".pdf", ".txt"))
            try:
                result = subprocess.run(
                    [PDFTOTEXT_EXE, pdf_path, txt_path],
                    check=True,
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    text=True
                )
                stderr_output = result.stderr.strip()
                if "Syntax Error" in stderr_output:
                    with open(error_log_path, "a", encoding="utf-8") as err_log:
                        err_log.write(f"{filename}: {stderr_output}\n")
            except Exception as e:
                print(f"[UNEXPECTED ERROR] {filename}: {e}")
                with open(error_log_path, "a", encoding="utf-8") as err_log:
                    err_log.write(f"{filename}\n")
            subprocess.run(["pdftotext", pdf_path, txt_path])
            # subprocess.run([PDFTOTEXT_EXE, pdf_path, txt_path], check=True)
        else:
            print(f"Skipping file: {filename}")

def search_files(keywords):
    keywords = [kw.lower().strip() for kw in keywords]

    with open("urls.txt", "r", encoding="utf-8") as urls, open(
        "dois.txt", "r", encoding="utf-8"
    ) as dois:

        info_dict = defaultdict(dict)
        doi_rdr = csv.reader(dois, delimiter=",")
        url_rdr = csv.reader(urls, delimiter=",")

        for line in doi_rdr:
            doi = line[0]
            info_dict[doi]["title"] = line[1]
            info_dict[doi]["year"] = str(line[2].strip())

        for line in url_rdr:
            doi = line[0]
            url = line[1].strip()
            filename_base = url.split('/')[-1][:-4]  # Strip '.pdf'
            pdf = f"{filename_base}pdf"
            text = f"{filename_base}txt"

            info_dict[doi]["url"] = url
            info_dict[doi]["pdf"] = pdf
            info_dict[doi]["text"] = text
            info_dict[doi]["extracted"] = False
            info_dict[doi]["tool"] = []

            for kw in keywords:
                info_dict[doi][kw] = []

        files = os.listdir("papers/pdf_texts")

        # Remove bad DOIs if present
        bad = [
            "10.21437/INTERSPEECH.2016-908600",
            "10.21437/INTERSPEECH.2018-3028",
            "10.21437/INTERSPEECH.2018-3015",
            "10.21437/INTERSPEECH.2019-8013",
        ]
        for key in bad:
            info_dict.pop(key, None)

        for doi in tqdm(info_dict.keys()):
            doc_title = info_dict[doi]["text"]
            if doc_title in files:
                info_dict[doi]["extracted"] = True
                with open(f"papers/pdf_texts/{doc_title}", "r", encoding="latin-1") as paper:
                    for line in paper:
                        line = line.lower()
                        for keyword in keywords:
                            if keyword in line:
                                info_dict[doi][keyword].append(line)

    return info_dict

def write_csv(info_dict):
    # add relevant keyword fields
    sample = next(iter(info_dict.values()))
    metadata = {"title", "year", "url", "pdf", "text", "extracted", "tool"}
    keyword_fields = [k for k in sample.keys() if k not in metadata]

    with open("paper_extracted_info.csv", "w", encoding="utf-8", newline="") as outfile:
        csvwriter = csv.writer(outfile)

        header = (
            ["doi", "title", "year"]
            + [kw for kw in keyword_fields]
            + ["tool", "url", "pdf", "text", "downloaded"]
        )
        csvwriter.writerow(header)

        for doi, info in info_dict.items():
            row = [
                doi,
                info.get("title", ""),
                info.get("year", ""),
            ]
            # add keyword matches
            for kw in keyword_fields:
                row.append("; ".join(info.get(kw, [])))

            row.extend([
                "; ".join(info.get("tool", [])),
                info.get("url", ""),
                info.get("pdf", ""),
                info.get("text", ""),
                info.get("extracted", False),
            ])
            csvwriter.writerow(row)

def main():
    parser = argparse.ArgumentParser(description="PDF processing pipeline")
    parser.add_argument("--query", type=str, help="Semantic Scholar query")
    parser.add_argument("--fetch", action="store_true", help="Fetch metadata from Semantic Scholar")
    parser.add_argument("--extract-dois", action="store_true", help="Extract DOIs from metadata")
    parser.add_argument("--resolve-urls", action="store_true", help="DOIs to URLs")
    parser.add_argument("--download", action="store_true", help="Download PDFs")
    parser.add_argument("--extract-text", action="store_true", help="PDFs to text")
    parser.add_argument("--keywords", type=str, help="Comma-separated list of keywords for text analysis")
    parser.add_argument("--analyze", action="store_true", help="Search for keywords in text")
    parser.add_argument("--write-csv", action="store_true", help="Write final CSV")
    parser.add_argument("--all", action="store_true", help="Run entire pipeline")
    args = parser.parse_args()

    if (args.fetch or args.all) and not args.query:
        parser.error("--query is required when running --fetch or --all")

    if args.fetch or args.all:
        get_references(args.query)

    if args.extract_dois or args.all:
        dois = extract_dois("papers.jsonl", "dois")
        print("DOIs extracted successfully")

    if args.resolve_urls or args.all:
        with open("dois.txt",encoding="utf-8") as f:
            dois = f.readlines()
        extract_urls(dois, "urls")
        print("URLs extracted successfully")

    if args.download or args.all:
        download_pdfs("urls.txt")
        print("PDFs downloaded successfully")

    if args.extract_text or args.all:
        extract_texts()
        print("Text extracted successfully")

    if any([args.analyze, args.write_csv, args.all]) and not args.keywords:
        parser.error("--keywords is required when running --analyze, --write-csv, or --all")

    if args.analyze or args.all:
        info_dict = search_files(args.keywords.split(","))

    if args.write_csv or args.all:
        write_csv(info_dict)
        print("CSV saved as paper_extracted_info.csv")


if __name__ == "__main__":
    main()
