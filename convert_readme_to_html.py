import markdown2
import os

# Ensure we are in the /app directory where README.md is expected
if os.getcwd() != '/app':
    os.chdir('/app')

with open('README.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

html_content = markdown2.markdown(md_content, extras=['tables', 'fenced-code-blocks', 'toc'])

with open('README.html', 'w', encoding='utf-8') as f:
    f.write("<!DOCTYPE html>\n")
    f.write("<html>\n")
    f.write("<head>\n")
    f.write("<meta charset=\"utf-8\">\n")
    f.write("<title>README</title>\n")
    # Basic styling for readability
    f.write("<style>\n")
    f.write("body { font-family: sans-serif; margin: 20px; line-height: 1.6; max-width: 800px; margin-left: auto; margin-right: auto; padding: 1em; }\n")
    f.write("h1, h2, h3, h4, h5, h6 { color: #333; margin-top: 1.5em; }\n")
    f.write("h1 { border-bottom: 1px solid #eee; padding-bottom: 0.3em; }\n")
    f.write("table { border-collapse: collapse; margin-bottom: 1em; width: 100%; }\n")
    f.write("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n")
    f.write("th { background-color: #f2f2f2; }\n")
    f.write("pre { background-color: #f5f5f5; padding: 10px; border: 1px solid #ddd; border-radius: 4px; overflow-x: auto; }\n")
    f.write("code { font-family: monospace; background-color: #f5f5f5; padding: 0.2em 0.4em; border-radius: 3px;}\n")
    f.write("pre code { background-color: transparent; padding: 0; border-radius: 0; border: none; }\n")
    f.write("a { color: #0366d6; text-decoration: none; }\n")
    f.write("a:hover { text-decoration: underline; }\n")
    f.write(".toc { border: 1px solid #eee; padding: 10px; margin-bottom: 20px; background-color: #f9f9f9; border-radius: 4px; }\n")
    f.write(".toc h2 { margin-top: 0; border-bottom: 1px solid #ddd; padding-bottom: 0.5em; }\n")
    f.write(".toc ul { list-style-type: none; padding-left: 0; }\n")
    f.write(".toc ul ul { padding-left: 20px; }\n")
    f.write("img { max-width: 100%; height: auto; }\n")
    f.write("</style>\n")
    f.write("</head>\n")
    f.write("<body>\n")
    f.write("<div class=\"toc\">\n")
    f.write("<h2>Table of Contents</h2>\n")
    f.write(markdown2.markdown(md_content, extras=['toc']).toc_html) # Generate TOC again for the specific TOC div
    f.write("</div>\n")
    f.write(html_content)
    f.write("</body>\n")
    f.write("</html>\n")
