#!/usr/bin/env python3
"""
StreamForge Comprehensive Engineering Report PDF Generator
Renders a publication-grade HTML document and compiles it to PDF using Microsoft Edge headless.
"""

import os
import sys
import subprocess
from pathlib import Path

WORKSPACE = Path(r"C:\Users\satya\OneDrive\Desktop\StreamForge")
DOCS_DIR = WORKSPACE / "docs"
OUTPUT_HTML = WORKSPACE / "StreamForge_Report.html"
OUTPUT_PDF = WORKSPACE / "StreamForge_Engineering_Report.pdf"
EDGE_EXE = Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")

def read_file(path: Path) -> str:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return ""

def md_to_html_simple(md_text: str) -> str:
    """Simple parser for markdown formatting into clean HTML."""
    lines = md_text.splitlines()
    html_lines = []
    in_code = False
    code_lang = ""
    in_table = False
    table_has_header = False

    for line in lines:
        stripped = line.strip()

        # Code blocks
        if stripped.startswith("```"):
            if in_code:
                html_lines.append("</code></pre>")
                in_code = False
            else:
                code_lang = stripped[3:].strip()
                html_lines.append(f'<pre><code class="language-{code_lang}">')
                in_code = True
            continue

        if in_code:
            import html
            html_lines.append(html.escape(line))
            continue

        # Tables
        if "|" in line and (stripped.startswith("|") or stripped.endswith("|")):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if not cells:
                continue
            # Divider line
            if all(re_match_divider(c) for c in cells):
                table_has_header = True
                continue
            
            if not in_table:
                html_lines.append('<div class="table-container"><table>')
                in_table = True
                table_has_header = False
                html_lines.append("<thead><tr>" + "".join(f"<th>{inline_format(c)}</th>" for c in cells) + "</tr></thead><tbody>")
            else:
                html_lines.append("<tr>" + "".join(f"<td>{inline_format(c)}</td>" for c in cells) + "</tr>")
            continue
        else:
            if in_table:
                html_lines.append("</tbody></table></div>")
                in_table = False

        if not stripped:
            html_lines.append("<p></p>")
            continue

        # Headings
        if stripped.startswith("###### "):
            html_lines.append(f"<h6>{inline_format(stripped[7:])}</h6>")
        elif stripped.startswith("##### "):
            html_lines.append(f"<h5>{inline_format(stripped[6:])}</h5>")
        elif stripped.startswith("#### "):
            html_lines.append(f"<h4>{inline_format(stripped[5:])}</h4>")
        elif stripped.startswith("### "):
            html_lines.append(f"<h3>{inline_format(stripped[4:])}</h3>")
        elif stripped.startswith("## "):
            html_lines.append(f"<h2>{inline_format(stripped[3:])}</h2>")
        elif stripped.startswith("# "):
            html_lines.append(f"<h1>{inline_format(stripped[2:])}</h1>")
        elif stripped.startswith("> "):
            html_lines.append(f'<div class="callout">{inline_format(stripped[2:])}</div>')
        elif stripped.startswith("- ") or stripped.startswith("* "):
            html_lines.append(f'<li class="bullet">{inline_format(stripped[2:])}</li>')
        elif len(stripped) > 2 and stripped[0].isdigit() and stripped[1] == ".":
            html_lines.append(f'<li class="numbered">{inline_format(stripped[2:].strip())}</li>')
        elif stripped == "---":
            html_lines.append("<hr/>")
        else:
            html_lines.append(f"<p>{inline_format(stripped)}</p>")

    if in_table:
        html_lines.append("</tbody></table></div>")
    if in_code:
        html_lines.append("</code></pre>")

    return "\n".join(html_lines)

def re_match_divider(cell: str) -> bool:
    c = cell.replace("-", "").replace(":", "").strip()
    return len(c) == 0

def inline_format(text: str) -> str:
    import re
    import html
    # Escape basic HTML first
    # Protect existing tags if any
    # Bold **text**
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    # Italic *text*
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    # Inline code `code`
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    return text

def build_report():
    print("Reading markdown documentation...")
    readme_md = read_file(WORKSPACE / "README.md")
    hld_md = read_file(DOCS_DIR / "HLD.md")
    lld_md = read_file(DOCS_DIR / "LLD.md")
    bench_md = read_file(DOCS_DIR / "BENCHMARKS.md")
    coverage_md = read_file(DOCS_DIR / "TEST_COVERAGE.md")
    interview_md = read_file(DOCS_DIR / "INTERVIEW_PREP.md")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>StreamForge — Complete Engineering Report</title>
<style>
  @page {{
    size: A4;
    margin: 18mm 16mm 20mm 16mm;
    @bottom-right {{
      content: counter(page);
    }}
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #1f2328;
    background-color: #ffffff;
    line-height: 1.55;
    font-size: 10.5pt;
    margin: 0;
    padding: 0;
  }}
  .cover-page {{
    page-break-after: always;
    display: flex;
    flex-direction: column;
    justify-content: center;
    min-height: 90vh;
    padding: 60px 40px;
    box-sizing: border-box;
    border-left: 8px solid #0969da;
    background: linear-gradient(180deg, #f6f8fa 0%, #ffffff 100%);
  }}
  .cover-title {{
    font-size: 32pt;
    font-weight: 800;
    color: #0969da;
    margin: 0 0 10px 0;
    letter-spacing: -0.5px;
  }}
  .cover-subtitle {{
    font-size: 16pt;
    font-weight: 500;
    color: #424a53;
    margin: 0 0 30px 0;
  }}
  .cover-badges {{
    margin-bottom: 40px;
  }}
  .badge {{
    display: inline-block;
    background: #0969da;
    color: #ffffff;
    font-size: 9pt;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 6px;
    margin-right: 8px;
    margin-bottom: 6px;
  }}
  .badge-secondary {{
    background: #57606a;
  }}
  .badge-success {{
    background: #1a7f37;
  }}
  .cover-meta {{
    margin-top: 60px;
    font-size: 10pt;
    color: #57606a;
    border-top: 1px solid #d0d7de;
    padding-top: 20px;
  }}
  .page-break {{
    page-break-after: always;
  }}
  h1 {{
    font-size: 18pt;
    font-weight: 700;
    color: #0969da;
    border-bottom: 2px solid #d0d7de;
    padding-bottom: 6px;
    margin-top: 28px;
    margin-bottom: 14px;
    page-break-after: avoid;
  }}
  h2 {{
    font-size: 14pt;
    font-weight: 600;
    color: #1f2328;
    border-bottom: 1px solid #eaeef2;
    padding-bottom: 4px;
    margin-top: 22px;
    margin-bottom: 10px;
    page-break-after: avoid;
  }}
  h3 {{
    font-size: 12pt;
    font-weight: 600;
    color: #24292f;
    margin-top: 16px;
    margin-bottom: 8px;
    page-break-after: avoid;
  }}
  h4, h5, h6 {{
    font-size: 11pt;
    font-weight: 600;
    color: #32383f;
    margin-top: 14px;
    margin-bottom: 6px;
  }}
  p {{
    margin: 6px 0;
    text-align: justify;
  }}
  code {{
    font-family: Consolas, "Liberation Mono", Menlo, monospace;
    font-size: 9pt;
    background-color: #f6f8fa;
    border: 1px solid #d0d7de;
    border-radius: 4px;
    padding: 1px 4px;
    color: #0550ae;
  }}
  pre {{
    background-color: #f6f8fa;
    border: 1px solid #d0d7de;
    border-radius: 6px;
    padding: 10px 12px;
    font-size: 8.5pt;
    overflow-x: auto;
    page-break-inside: avoid;
    line-height: 1.4;
  }}
  pre code {{
    background-color: transparent;
    border: none;
    padding: 0;
    color: #24292f;
  }}
  .table-container {{
    margin: 12px 0;
    page-break-inside: avoid;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 8.5pt;
    margin: 6px 0;
  }}
  th, td {{
    border: 1px solid #d0d7de;
    padding: 5px 8px;
    text-align: left;
  }}
  th {{
    background-color: #f6f8fa;
    font-weight: 600;
    color: #24292f;
  }}
  tr:nth-child(even) {{
    background-color: #fcfcfd;
  }}
  .callout {{
    background-color: #f0f7ff;
    border-left: 4px solid #0969da;
    padding: 8px 12px;
    margin: 10px 0;
    border-radius: 0 4px 4px 0;
    font-size: 9.5pt;
  }}
  li.bullet {{
    list-style-type: disc;
    margin-left: 20px;
    margin-bottom: 3px;
  }}
  li.numbered {{
    list-style-type: decimal;
    margin-left: 20px;
    margin-bottom: 3px;
  }}
  .metric-card-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin: 14px 0;
  }}
  .metric-card {{
    flex: 1;
    min-width: 130px;
    background: #f6f8fa;
    border: 1px solid #d0d7de;
    border-radius: 6px;
    padding: 10px;
    text-align: center;
  }}
  .metric-value {{
    font-size: 16pt;
    font-weight: 700;
    color: #0969da;
    margin-bottom: 2px;
  }}
  .metric-label {{
    font-size: 8pt;
    color: #57606a;
    font-weight: 600;
    text-transform: uppercase;
  }}
</style>
</head>
<body>

<!-- COVER PAGE -->
<div class="cover-page">
  <div class="cover-title">StreamForge</div>
  <div class="cover-subtitle">A Kafka-Inspired, Durable Message Broker in C++17<br/>Built Natively for Microsoft Windows</div>
  
  <div class="cover-badges">
    <span class="badge">Modern C++17</span>
    <span class="badge">Native Winsock2</span>
    <span class="badge">Win32 File I/O & Memory Maps</span>
    <span class="badge badge-secondary">Zero External Dependencies</span>
    <span class="badge badge-success">Milestones 1–7 Complete</span>
  </div>

  <div class="metric-card-row">
    <div class="metric-card">
      <div class="metric-value">87,037</div>
      <div class="metric-label">Peak Produce Rec/Sec</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">65,044</div>
      <div class="metric-label">Peak Fetch Rec/Sec</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">3.86 ms</div>
      <div class="metric-label">p99 Latency (1K Sockets)</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">17</div>
      <div class="metric-label">Constant Server Threads</div>
    </div>
    <div class="metric-card">
      <div class="metric-value">100%</div>
      <div class="metric-label">Test Pass Rate (7/7 Suites)</div>
    </div>
  </div>

  <div class="cover-meta">
    <strong>Author / Developer:</strong> StreamForge Engineering Team<br/>
    <strong>Target Environment:</strong> Microsoft Windows 10 / 11 64-bit | MinGW GCC UCRT64 / MSVC<br/>
    <strong>Verification Date:</strong> September 2026<br/>
    <strong>Document Status:</strong> Final Project Engineering Report & Defense Portfolio
  </div>
</div>

<!-- TABLE OF CONTENTS -->
<h1>Table of Contents</h1>
<ul>
  <li class="bullet"><strong>Chapter 1: Executive Summary & Project Highlights</strong></li>
  <li class="bullet"><strong>Chapter 2: High-Level System Architecture (HLD)</strong></li>
  <li class="bullet"><strong>Chapter 3: Low-Level Design & Component Internals (LLD)</strong></li>
  <li class="bullet"><strong>Chapter 4: On-Disk Formats & Concurrency Lock Inventory</strong></li>
  <li class="bullet"><strong>Chapter 5: Empirical Benchmark Suite & Real Hardware Metrics</strong></li>
  <li class="bullet"><strong>Chapter 6: Quality Assurance & Test Coverage Matrix</strong></li>
  <li class="bullet"><strong>Chapter 7: Technical Interview Preparation & Architectural Defense</strong></li>
</ul>

<div class="page-break"></div>

<!-- CHAPTER 1: README / SUMMARY -->
<h1>Chapter 1: Executive Summary & Project Highlights</h1>
{md_to_html_simple(readme_md)}

<div class="page-break"></div>

<!-- CHAPTER 2: HLD -->
<h1>Chapter 2: High-Level Architecture (HLD)</h1>
{md_to_html_simple(hld_md)}

<div class="page-break"></div>

<!-- CHAPTER 3 & 4: LLD -->
<h1>Chapter 3: Low-Level Design (LLD) & Lock Inventory</h1>
{md_to_html_simple(lld_md)}

<div class="page-break"></div>

<!-- CHAPTER 5: BENCHMARKS -->
<h1>Chapter 4: Benchmark Suite & Real Empirical Results</h1>
{md_to_html_simple(bench_md)}

<div class="page-break"></div>

<!-- CHAPTER 6: TEST COVERAGE -->
<h1>Chapter 5: Test Coverage & Verification Matrix</h1>
{md_to_html_simple(coverage_md)}

<div class="page-break"></div>

<!-- CHAPTER 7: INTERVIEW PREP -->
<h1>Chapter 6: Technical Interview Preparation & Defense</h1>
{md_to_html_simple(interview_md)}

</body>
</html>
"""

    print(f"Writing HTML report to {OUTPUT_HTML}...")
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Compiling PDF via Microsoft Edge Headless to {OUTPUT_PDF}...")
    cmd = [
        str(EDGE_EXE),
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={OUTPUT_PDF}",
        str(OUTPUT_HTML)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0 and OUTPUT_PDF.exists():
        size_mb = OUTPUT_PDF.stat().st_size / (1024 * 1024)
        print(f"PDF generated successfully: {OUTPUT_PDF} ({size_mb:.2f} MB)")
        return True
    else:
        print(f"Edge PDF generation returned {res.returncode}: {res.stderr}")
        return False

if __name__ == "__main__":
    success = build_report()
    sys.exit(0 if success else 1)
