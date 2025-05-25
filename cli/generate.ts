import { execSync } from 'child_process';
import { join } from 'path';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';

interface GenerateSubsetSchemaConfig {
  schema: string;
  models: string[] | "*";
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
  const subsetSchema = `
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

        // Convert ObjectId and other MongoDB types to String
        const convertedContent = modelContent
          .split('\n')
          .map(line => {
            let converted = line
              .replace(/@db\.ObjectId/g, '')
              .replace(/@db\.String/g, '')
              .replace(/@default\(auto\(\)\)/g, '@default(uuid())')
              .trim();
            
            if (converted && !converted.startsWith('//')) {
              return converted;
            }
            return line.trim();
          })
          .filter(line => line)
          .join('\n');

        return `model ${modelName} {
${convertedContent}
}`;
      })
      .join('\n\n')}
`;

  // Write subset schema
  const schemaPath = join(output, 'block.prisma');
  writeFileSync(schemaPath, subsetSchema);

  console.log('Generated schema at:', schemaPath);

  // Generate Prisma client and create template database
  try {
    // Change to output directory

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