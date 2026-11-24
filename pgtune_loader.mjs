import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

export async function resolve(specifier, context, nextResolve) {
  try {
    return await nextResolve(specifier, context)
  } catch (error) {
    if (!specifier.startsWith('.') || !context.parentURL) throw error
    const base = new URL(specifier, context.parentURL)
    for (const suffix of ['.js', '.mjs', '.json']) {
      const candidate = new URL(base.href + suffix)
      if (existsSync(fileURLToPath(candidate))) {
        return { url: candidate.href, shortCircuit: true }
      }
    }
    throw error
  }
}