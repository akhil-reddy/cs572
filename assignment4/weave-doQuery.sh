#!/bin/bash

# Search for drift phonk with aggressive bass
curl -X POST -H "Content-Type: application/json" -d '{"query":"{ Get { PhonkMusic(limit: 3 nearText: {concepts: [\"drift aggressive bass racing\"]}) { title artist subgenre characteristics } } }"}' http://localhost:8080/v1/graphql