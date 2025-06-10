import { execSync } from 'child_process';
import { join } from 'path';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';

interface GenerateSubsetSchemaConfig {
  schema: string;
  models: string[] | "*";
}

function cleanAttribute(attr: string): string {
  // Handle relation attributes
  if (attr.includes('@relation')) {
    const parts = attr.split('@relation')[1].trim();
    // Remove any trailing comma before the closing parenthesis
    const cleanedParts = parts.replace(/,\s*\)/g, ')');
    const args = cleanedParts.slice(1, -1).split(',').map(p => p.trim()).filter(p => p);
    return `@relation(${args.join(', ')})`;
  }
  // Handle index attributes
  if (attr.includes('@@index')) {
    const parts = attr.split('@@index')[1].trim();
    // Remove any trailing comma before the closing parenthesis
    const cleanedParts = parts.replace(/,\s*\)/g, ')');
    const args = cleanedParts.slice(1, -1).split(',').map(p => p.trim()).filter(p => p);
    return `@@index(${args.join(', ')})`;
  }
  return attr;
}

function parseModelContent(content: string): { fields: string[], relations: Map<string, string[]> } {
  const lines = content.split('\n');
  const fields: string[] = [];
  const relations = new Map<string, string[]>();
  let currentField = '';
  let currentFieldLine = '';

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines and comments
    if (!line || line.startsWith('//')) {
      continue;
    }

    // Process the line for SQLite compatibility
    let processed = line
      .replace(/@db\.ObjectId/g, '')
      .replace(/@db\.String/g, '')
      .replace(/@db\.Timestamptz(\(\d+\))?/g, '')
      .replace(/@default\(auto\(\)\)/g, '@default(uuid())')
      .replace(/@db\.Uuid/g, '')
      .replace(/@db\.Date/g, '')
      .replace(/@db\.Decimal/g, '')
      .replace(/map: "[^"]+"/g, '')
      .replace(/String\[\]/g, 'String')
      .replace(/@default\(\[[^\]]+\]\)/g, '@default("[]")')
      .replace(/@default\(\[\]\)/g, '@default("[]")')
      // SQLite compatibility for gen_random_uuid()
      .replace(/@default\(dbgenerated\("gen_random_uuid\(\)"\)\)/g, '@default(uuid())')
      // Remove any other dbgenerated defaults
      .replace(/@default\(dbgenerated\("[^"]+"\)\)/g, '');

    // If this is a field definition (not a model or enum)
    if (!processed.startsWith('model') && !processed.startsWith('enum')) {
      // Extract the field name (everything before the first space)
      const fieldName = processed.split(' ')[0];
      currentField = fieldName;
      currentFieldLine = processed;
      fields.push(currentFieldLine);
    }

    // If line starts with @relation or @@index, add it to the current field's relations
    if (line.startsWith('@relation') || line.startsWith('@@index')) {
      if (currentField) {
        const fieldRelations = relations.get(currentField) || [];
        // Clean the relation/index attribute by removing trailing commas
        const cleanedAttr = line.replace(/,\s*\)/g, ')');
        fieldRelations.push(cleanedAttr);
        relations.set(currentField, fieldRelations);
      }
    }
  }

  return { fields, relations };
}

function processModelContent(content: string): string {
  const { fields, relations } = parseModelContent(content);
  
  // Combine fields with their relations
  const processedLines = fields.map(field => {
    const fieldName = field.split(' ')[0];
    const fieldRelations = relations.get(fieldName) || [];
    
    // If the field already has relations in its definition, don't add them again
    if (field.includes('@relation') || field.includes('@@index')) {
      return field;
    }
    
    // Add relations on the same line as the field
    return field + (fieldRelations.length > 0 ? ' ' + fieldRelations.join(' ') : '');
  });

  return processedLines.join('\n');
}

export async function generate(config: GenerateSubsetSchemaConfig) {
  const { schema, models } = config;
  const output = join('.', 'prisma', 'blocks')

  // Write blocks to .gitignore if blocks not already in .gitignore
  // also if the file does not exist, create it
  const gitignorePath = join('.', '.gitignore');
  if (!existsSync(gitignorePath)) {
    writeFileSync(gitignorePath, 'prisma/blocks\n');
  } else {
    const gitignoreContent = readFileSync(gitignorePath, 'utf-8');
    // if blocks is not in the file, make sure youre checking if its standalone not part of another ignore
    if (!gitignoreContent.includes('\nprisma/blocks\n') && !gitignoreContent.includes('prisma/blocks')) {
      writeFileSync(gitignorePath, `${gitignoreContent}\nprisma/blocks\n`);
    }
  }

  if (!existsSync(output)) {
    mkdirSync(output, { recursive: true });
  }

  console.log('Generating subset schema:');
  console.log('- Main schema:', schema);
  console.log('- Output dir:', output);

  const mainSchema = readFileSync(schema, 'utf-8').replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');

  // Extract model definitions, enums, and other schema elements
  const modelRegex = /model\s+(\w+)\s*{([^}]+)}/g;
  const enumRegex = /enum\s+(\w+)\s*{([^}]+)}/g;
  const modelsMap = new Map<string, string>();
  const enumsMap = new Map<string, string>();
  let match;

  // Extract enums first
  while ((match = enumRegex.exec(mainSchema)) !== null) {
    const enumName = match[1];
    const enumContent = match[2];
    enumsMap.set(enumName, enumContent);
  }

  // Extract models
  while ((match = modelRegex.exec(mainSchema)) !== null) {
    const modelName = match[1];
    const modelContent = match[2];
    modelsMap.set(modelName, modelContent);
  }

  // Create subset schema
  let subsetSchema = `
generator client {
  provider = "prisma-client-js"
  output   = "./generated/block"
}

datasource db {
  provider = "sqlite"
  url      = "file:template.sqlite"
}

${Array.from(enumsMap.entries())
    .map(([enumName, enumContent]) => `enum ${enumName} {${enumContent}}`)
    .join('\n\n')}

${(models === "*" ? Array.from(modelsMap.keys()) : models)
      .map((modelName: string) => {
        const modelContent = modelsMap.get(modelName);
        if (!modelContent) {
          throw new Error(`Model ${modelName} not found in schema`);
        }

        const convertedContent = processModelContent(modelContent);

        return `model ${modelName} {
${convertedContent}
}`;
      })
      .join('\n\n')}
`;

  // Clean trailing commas in relations and indexes
  subsetSchema = subsetSchema
    .replace(/,\s*\)/g, ')')  // Remove trailing commas in relations
    .replace(/,\s*}/g, '}')   // Remove trailing commas in model definitions
    .replace(/,\s*]/g, ']');  // Remove trailing commas in arrays

  // Write subset schema
  const schemaPath = join(output, 'block.prisma');
  writeFileSync(schemaPath, subsetSchema);

  console.log('Generated schema at:', schemaPath);

  // Generate Prisma client and create template database
  try {
    console.log("Exists", existsSync(schemaPath))

    // Run Prisma commands with explicit schema path
    execSync(`npx prisma generate --schema=${schemaPath}`, { stdio: 'inherit' });
    execSync(`npx prisma db push --schema=${schemaPath}`, { stdio: 'inherit' });

    return {
      schemaPath,
      templatePath: join(output, 'template.sqlite'),
    };
  } catch (error) {
    throw new Error(`Failed to generate Prisma client: ${error}`);
  }
}