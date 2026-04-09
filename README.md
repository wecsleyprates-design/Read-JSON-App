# JSON Data Analyzer

A full-stack application to ingest, validate, analyze, and present data from JSON files.

## Tech Stack
- **Backend**: Express.js (Node.js)
- **Frontend**: React, Vite, Tailwind CSS
- **Visualization**: Recharts
- **Validation**: Zod (for schema validation, optional)

## Features
1. **JSON Ingestion**: Upload JSON files via the UI.
2. **Data Transformation**: Flattens nested JSON structures into a tabular format.
3. **Analysis Layer**: Generates summary statistics and column profiling (data types, null counts, unique counts).
4. **Interactive UI**: View data in a paginated table and analyze distributions with interactive bar charts.
5. **Export**: Download the processed data as a JSON file.

## Setup Instructions

### 1. Install Dependencies
```bash
npm install
```

### 2. Run Locally
```bash
npm run dev
```
This will start the full-stack application (Express backend + Vite frontend) on `http://localhost:3000`.

### 3. Build for Production
```bash
npm run build
npm start
```

## Example API Calls

You can interact with the backend API directly using tools like `curl` or Postman.

### Upload JSON
```bash
curl -X POST http://localhost:3000/api/upload \
  -F "file=@sample.json"
```

### Get Summary
```bash
curl http://localhost:3000/api/summary
```

### Get Paginated Data
```bash
curl "http://localhost:3000/api/data?page=1&limit=10"
```

### Get Chart Data
```bash
curl "http://localhost:3000/api/charts?column=age"
```

## Sample Data
A `sample.json` file is included in the root directory for testing. You can upload this file via the UI to see the application in action.
