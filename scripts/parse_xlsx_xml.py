import xml.etree.ElementTree as ET
from pathlib import Path

root = Path('documents/xlsx_extracted/xl')
ss_file = root / 'sharedStrings.xml'
ws_dir = root / 'worksheets'

# load shared strings
ss = []
if ss_file.exists():
    tree = ET.parse(ss_file)
    sst = tree.getroot()
    ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    for si in sst.findall('ns:si', ns):
        texts = []
        for t in si.findall('.//ns:t', ns):
            texts.append(t.text or '')
        ss.append(''.join(texts))

# parse sheet1
rows = []
if ws_dir.exists():
    ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    for ws_file in sorted(ws_dir.glob('sheet*.xml')):
        tree = ET.parse(ws_file)
        ws = tree.getroot()
        sheet_name = ws_file.name
        for row in ws.findall('.//ns:row', ns):
            row_idx = int(row.attrib.get('r', '0'))
            cols = {}
            for c in row.findall('ns:c', ns):
                r = c.attrib.get('r')
                col = ''.join([ch for ch in r if ch.isalpha()])
                val = ''
                t = c.attrib.get('t')
                v = c.find('ns:v', ns)
                if v is not None and v.text is not None:
                    if t == 's':
                        idx = int(v.text)
                        val = ss[idx] if idx < len(ss) else f'<ss_idx_{idx}>'
                    else:
                        val = v.text
                else:
                    is_elem = c.find('ns:is', ns)
                    if is_elem is not None:
                        texts = [t.text or '' for t in is_elem.findall('.//ns:t', ns)]
                        val = ''.join(texts)
                cols[col] = val
            rows.append((sheet_name, row_idx, cols))

# print rows in tabular form for inspection
for item in rows:
    sheet_name, r_idx, cols = item
    out = [sheet_name, str(r_idx)] + [cols.get(ch, '') for ch in ['A','B','C','D','E','F','G','H','I','J','K']]
    # print only rows that contain likely IO addresses or tags
    row_text = '\t'.join(out).lower()
    if any(x in row_text for x in ['i0.', 'i1.', 'i2.', 'q0.', 'out', 'in', 'vacuum', 'kasa', 'vacuum', 'light', 'ışık', 'kasa']):
        print('\t'.join(out))
