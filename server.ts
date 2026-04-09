import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import cors from 'cors';
import multer from 'multer';
import { z } from 'zod';

const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 } // 50MB limit
});

// Simple in-memory cache
class Cache {
  private store = new Map<string, { data: any, timestamp: number }>();
  private ttl = 5 * 60 * 1000; // 5 minutes

  get(key: string) {
    const item = this.store.get(key);
    if (!item) return null;
    if (Date.now() - item.timestamp > this.ttl) {
      this.store.delete(key);
      return null;
    }
    return item.data;
  }

  set(key: string, data: any) {
    this.store.set(key, { data, timestamp: Date.now() });
  }

  clear() {
    this.store.clear();
  }
}

const apiCache = new Cache();

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(cors());
  app.use(express.json({ limit: '50mb' }));

  // In-memory storage for the uploaded data
  let currentData: any[] = [];
  let currentSummary: any = null;

  // --- API Routes ---

  app.get('/api/health', (req, res) => {
    res.json({ status: 'ok' });
  });

  app.post('/api/upload', upload.single('file'), (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
      }

      const fileContent = req.file.buffer.toString('utf-8');
      let jsonData;
      try {
        jsonData = JSON.parse(fileContent);
      } catch (e) {
        return res.status(400).json({ error: 'Invalid JSON format' });
      }

      // Ensure it's an array
      if (!Array.isArray(jsonData)) {
        jsonData = [jsonData];
      }

      // Flatten nested JSON
      const flattenObject = (obj: any, prefix = ''): any => {
        return Object.keys(obj).reduce((acc: any, k: string) => {
          const pre = prefix.length ? prefix + '_' : '';
          if (typeof obj[k] === 'object' && obj[k] !== null && !Array.isArray(obj[k])) {
            Object.assign(acc, flattenObject(obj[k], pre + k));
          } else {
            acc[pre + k] = obj[k];
          }
          return acc;
        }, {});
      };

      const flattenedData = jsonData.map(item => flattenObject(item));
      currentData = flattenedData;
      apiCache.clear(); // Clear cache on new upload

      // Generate Summary
      if (flattenedData.length === 0) {
        currentSummary = { totalRows: 0, columns: [], qualityScore: 0, pydanticSchema: '' };
        return res.json({ message: 'File uploaded successfully, but it is empty', summary: currentSummary });
      }

      const columns = Array.from(new Set(flattenedData.flatMap(row => Object.keys(row))));
      
      let totalCells = flattenedData.length * columns.length;
      let missingCells = 0;
      let consistentCells = 0;

      const columnProfiles = columns.map(col => {
        const values = flattenedData.map(row => row[col]);
        const nonNullValues = values.filter(v => v !== null && v !== undefined && v !== '');
        const uniqueValues = new Set(nonNullValues);
        
        missingCells += (values.length - nonNullValues.length);

        let type = 'string';
        let pyType = 'str';
        if (nonNullValues.length > 0) {
          const allNumbers = nonNullValues.every(v => typeof v === 'number' || (!isNaN(Number(v)) && typeof v !== 'boolean'));
          if (allNumbers) {
             type = 'number';
             pyType = 'float';
          } else {
             const allBooleans = nonNullValues.every(v => typeof v === 'boolean' || v === 'true' || v === 'false');
             if (allBooleans) {
                type = 'boolean';
                pyType = 'bool';
             }
          }
        }

        // Calculate consistency for this column
        let consistentCount = 0;
        nonNullValues.forEach(v => {
           if (type === 'number' && (typeof v === 'number' || !isNaN(Number(v)))) consistentCount++;
           else if (type === 'boolean' && (typeof v === 'boolean' || v === 'true' || v === 'false')) consistentCount++;
           else if (type === 'string') consistentCount++;
        });
        consistentCells += consistentCount;

        // Determine semantic group
        let semanticGroup = 'Other';
        const lowerCol = col.toLowerCase();
        if (lowerCol.includes('id') || lowerCol.includes('uuid') || lowerCol.includes('hash') || lowerCol.includes('token')) {
            semanticGroup = 'Identifiers';
        } else if (lowerCol.includes('date') || lowerCol.includes('time') || lowerCol.includes('created') || lowerCol.includes('updated')) {
            semanticGroup = 'Date & Time';
        } else if (lowerCol.includes('address') || lowerCol.includes('city') || lowerCol.includes('state') || lowerCol.includes('zip') || lowerCol.includes('postal') || lowerCol.includes('country') || lowerCol.includes('region') || lowerCol.includes('lat') || lowerCol.includes('lon')) {
            semanticGroup = 'Location';
        } else if (type === 'number') {
            semanticGroup = 'Metrics & Numbers';
        } else if (type === 'boolean') {
            semanticGroup = 'Flags & Booleans';
        } else {
            semanticGroup = 'Text & Categories';
        }

        return {
          name: col,
          type,
          pyType,
          semanticGroup,
          nullCount: values.length - nonNullValues.length,
          uniqueCount: uniqueValues.size,
        };
      });

      const completeness = totalCells > 0 ? ((totalCells - missingCells) / totalCells) * 100 : 0;
      const consistency = (totalCells - missingCells) > 0 ? (consistentCells / (totalCells - missingCells)) * 100 : 0;
      const qualityScore = (completeness + consistency) / 2;

      // Generate Pydantic Schema
      let pydanticSchema = `from pydantic import BaseModel, Field\nfrom typing import Optional, Any\n\nclass InferredSchema(BaseModel):\n`;
      columnProfiles.forEach(col => {
         // Sanitize column name for python variable
         const safeName = col.name.replace(/[^a-zA-Z0-9_]/g, '_');
         const finalName = /^[0-9]/.test(safeName) ? `field_${safeName}` : safeName;
         
         if (finalName !== col.name) {
             pydanticSchema += `    ${finalName}: Optional[${col.pyType}] = Field(alias="${col.name}")\n`;
         } else {
             pydanticSchema += `    ${finalName}: Optional[${col.pyType}]\n`;
         }
      });

      currentSummary = {
        totalRows: flattenedData.length,
        columns: columnProfiles,
        qualityScore: Math.round(qualityScore * 100) / 100,
        completeness: Math.round(completeness * 100) / 100,
        consistency: Math.round(consistency * 100) / 100,
        pydanticSchema
      };

      res.json({ message: 'File uploaded and processed successfully', summary: currentSummary });
    } catch (error: any) {
      console.error('Upload error:', error);
      res.status(500).json({ error: error.message || 'Internal server error' });
    }
  });

  app.get('/api/summary', (req, res) => {
    if (!currentSummary) {
      return res.status(404).json({ error: 'No data available. Please upload a file first.' });
    }
    res.json(currentSummary);
  });

  app.get('/api/data', (req, res) => {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = (req.query.search as string) || '';
    
    const cacheKey = `data_${page}_${limit}_${search}`;
    const cached = apiCache.get(cacheKey);
    if (cached) {
       return res.json(cached);
    }

    let filteredData = currentData;
    if (search) {
       const lowerSearch = search.toLowerCase();
       filteredData = currentData.filter(row => 
          Object.values(row).some(val => String(val).toLowerCase().includes(lowerSearch))
       );
    }

    const startIndex = (page - 1) * limit;
    const endIndex = page * limit;
    const paginatedData = filteredData.slice(startIndex, endIndex);

    const responseData = {
      data: paginatedData,
      total: filteredData.length,
      page,
      limit,
      totalPages: Math.ceil(filteredData.length / limit)
    };

    apiCache.set(cacheKey, responseData);
    res.json(responseData);
  });

  app.get('/api/charts', (req, res) => {
    if (!currentData || currentData.length === 0) {
      return res.status(404).json({ error: 'No data available' });
    }

    const col = req.query.column as string;
    if (!col) {
      return res.status(400).json({ error: 'Column parameter is required' });
    }

    const cacheKey = `charts_${col}`;
    const cached = apiCache.get(cacheKey);
    if (cached) {
       return res.json(cached);
    }

    const colProfile = currentSummary?.columns.find((c: any) => c.name === col);
    const semanticGroup = colProfile?.semanticGroup || 'Other';
    const type = colProfile?.type || 'string';

    const values = currentData.map(row => row[col]).filter(v => v !== null && v !== undefined && v !== '');
    
    let responsePayload: any = { chartType: 'bar', data: [] };

    if (semanticGroup === 'Date & Time') {
      // Group by date
      const dateCounts: Record<string, number> = {};
      values.forEach(v => {
        let dateStr = String(v).substring(0, 10); // fallback
        try {
          const d = new Date(v);
          if (!isNaN(d.getTime())) {
            dateStr = d.toISOString().split('T')[0];
          }
        } catch(e) {}
        dateCounts[dateStr] = (dateCounts[dateStr] || 0) + 1;
      });
      responsePayload.chartType = 'line';
      responsePayload.data = Object.entries(dateCounts)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => a.name.localeCompare(b.name));

    } else if (type === 'number' && new Set(values).size > 10) {
      // Histogram for continuous numbers
      const numValues = values.map(Number).filter(n => !isNaN(n));
      if (numValues.length > 0) {
        const min = Math.min(...numValues);
        const max = Math.max(...numValues);
        if (min !== max) {
          const binCount = 10;
          const binSize = (max - min) / binCount;
          const bins = Array.from({length: binCount}, (_, i) => ({
            name: `${(min + i * binSize).toFixed(1)} - ${(min + (i + 1) * binSize).toFixed(1)}`,
            min: min + i * binSize,
            max: min + (i + 1) * binSize,
            value: 0
          }));
          numValues.forEach(num => {
            for (let i = 0; i < binCount; i++) {
              if (num >= bins[i].min && (num < bins[i].max || (i === binCount - 1 && num <= bins[i].max))) {
                bins[i].value++;
                break;
              }
            }
          });
          responsePayload.chartType = 'histogram';
          responsePayload.data = bins;
        }
      }
    }

    // Fallback to Bar chart for categorical data
    if (responsePayload.data.length === 0) {
      const counts: Record<string, number> = {};
      values.forEach(v => {
        const key = String(v);
        counts[key] = (counts[key] || 0) + 1;
      });

      responsePayload.chartType = 'bar';
      responsePayload.data = Object.entries(counts)
        .map(([name, value]) => ({ name, value }))
        .sort((a, b) => b.value - a.value)
        .slice(0, 20);
    }

    apiCache.set(cacheKey, responsePayload);
    res.json(responsePayload);
  });

  // --- Vite Middleware ---
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  // Error handling middleware to prevent HTML error pages
  app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
    console.error('Unhandled error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  });

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
