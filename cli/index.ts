#!/usr/bin/env node

import { generate } from './generate';

interface GenerateSubsetSchemaConfig {
  schema: string;
  models: string[] | "*";
}

// CLI entry point
if (require.main === module) {
  const args = process.argv.slice(2);
  const command = args[0];

  switch (command) {
    case 'generate': {
      const options: GenerateSubsetSchemaConfig = {
        schema: '',
        models: [],
      };

      let allModels = false;

      console.log(args);

      for (const arg of args.slice(1)) {
        if (arg.startsWith('--schema=')) {
          options.schema = arg.split('=')[1];
        } else if (arg.startsWith('--models=')) {
          options.models = arg.split('=')[1].split(',');
        } else if (arg === '--all-models') {
          allModels = true;
        }
      }

      console.log(allModels, options.models, options.schema);
      if (!options.schema || (!allModels && !options.models.length)) {
        console.error(
          '\x1b[31mUsage: npx sharded generate --schema=./prisma/schema.prisma [--models=Model1,Model2 | --all-models]\x1b[0m'
        );
        process.exit(1);
      }

      generate({
        schema: options.schema,
        models: allModels ? "*" : options.models,
      }).catch((error) => {
        console.error('\x1b[31mError:\x1b[0m', error.message);
        process.exit(1);
      });
      break;
    }
    default:
      console.error('\x1b[31mUnknown command:\x1b[0m', command);
      console.error('\x1b[36mAvailable commands:\x1b[0m');
      console.error('\x1b[32m  generate    Generate a subset schema for specified models\x1b[0m');
      process.exit(1);
  }
}