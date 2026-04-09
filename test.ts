import fs from 'fs';

const fileContent = fs.readFileSync('sample2.json', 'utf-8');
let jsonData = JSON.parse(fileContent);

if (!Array.isArray(jsonData)) {
  jsonData = [jsonData];
}

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

try {
  const flattenedData = jsonData.map((item: any) => flattenObject(item));
  console.log('Success, length:', flattenedData.length);
} catch (e) {
  console.error(e);
}
