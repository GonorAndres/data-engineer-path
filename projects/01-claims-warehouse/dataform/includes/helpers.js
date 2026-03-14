// Reusable macros for the claims warehouse Dataform project.

/**
 * Generate a surrogate integer key from one or more columns.
 * Uses FARM_FINGERPRINT for deterministic hashing.
 */
function surrogateKey(columns) {
  const parts = columns.map(c => `CAST(${c} AS STRING)`).join(", '|', ");
  return `FARM_FINGERPRINT(CONCAT(${parts}))`;
}

/**
 * Generate a date key integer (YYYYMMDD) from a date column.
 */
function dateKey(dateColumn) {
  return `CAST(FORMAT_DATE('%Y%m%d', ${dateColumn}) AS INT64)`;
}

/**
 * Schema name based on environment variable.
 * dev -> dev_<schema>, prod -> <schema>
 */
function envSchema(schema) {
  return `\${dataform.projectConfig.vars.env === "prod" ? "${schema}" : "dev_${schema}"}`;
}

module.exports = { surrogateKey, dateKey, envSchema };
