# PDF table scraper

PDF table to csv extraction tool. Requires the user to specify expected column headers for accurate extraction.

##  Quick start

### 1. Setup
```bash
cd ~/coeqwal-backend/scripts/pdf_scraper

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
./install_pdf_scraper.sh
```

### 2. Extract tables (manually specify headers)
```bash
# Activate environment
source venv/bin/activate

# Extract with your expected headers
python extract_tables_to_csv.py \
  --input ./pdf/your_file.pdf \
  --pages 1-5 \
  --headers "Column1,Column2,Column3,Column4"
```

##  Example

For the CalSim3 non-project agricultural demand units PDF:

```bash
python extract_tables_to_csv.py \
  --input ./pdf/ag_demand_units_np_sac.pdf \
  --pages 1-2 \
  --headers "Demand Unit,Diversion Arc,River Reach,River Mile,Bank,Area (acres),Annual Diversion (TAF)"
```

##  Command options

- **`--input`** (required): Path to PDF file
- **`--pages`** (required): Page range like "1-5" or "1,3,5-7"  
- **`--headers`** (required): Expected column headers (comma-separated)
- **`--output`**: Output directory (default: ./extracted_tables)
- **`--verbose`**: Show detailed extraction process

##  How it works

1. **Header detection**: Finds table headers using provided column names
2. **Column clustering**: Uses header positions to determine column boundaries
3. **Row extraction**: Groups text into rows, handling continuation lines
4. **Data cleaning**: Merges multi-line cells, removes boilerplate text <= nested columns typically need a manual adjustment
5. **csv output**: Structured data ready for database import

##  Tips

### **Getting headers rights**
- Use the same capitalization and punctuation as in the PDF
- Use quotes around the entire header list
- Separate headers with commas (no spaces after commas needed)