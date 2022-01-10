module.exports = {
  client: {
    includes: ['./**/*.{tsx,ts}'],
    localSchemaFile: 'schema.graphql',
    service: {
      localSchemaFile: './schema.graphql',
    },
  },
}
