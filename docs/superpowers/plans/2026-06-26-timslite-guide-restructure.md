# timslite-guide Restructure Plan

## Overview

Restructure the `skills/timslite-guide` directory to separate language-specific content into dedicated subdirectories while keeping universal content in the main SKILL.md.

## Current Structure

```
skills/timslite-guide/
├── SKILL.md              # Main entry (300 lines) - mixed universal + language-specific
├── api-reference.md      # Complete API reference (Rust-focused)
├── scenarios.md          # Feature scenarios (Rust + Python examples)
└── troubleshooting.md    # Common errors and solutions
```

## Target Structure

```
skills/timslite-guide/
├── SKILL.md                    # Universal content + language directory links
├── rust/
│   ├── SKILL.md                # Rust entry point
│   ├── quick-start.md          # Rust quick start guide
│   ├── api-reference.md        # Rust API signatures
│   └── examples.md             # Rust code examples
├── python/
│   ├── SKILL.md                # Python entry point
│   ├── quick-start.md          # Python quick start guide
│   ├── api-reference.md        # Python API signatures
│   └── examples.md             # Python code examples
├── nodejs/
│   ├── SKILL.md                # Node.js entry point
│   ├── quick-start.md          # Node.js quick start guide
│   ├── api-reference.md        # Node.js API signatures
│   └── examples.md             # Node.js code examples
├── java/
│   ├── SKILL.md                # Java entry point
│   ├── quick-start.md          # Java quick start guide
│   ├── api-reference.md        # Java API signatures
│   └── examples.md             # Java code examples
└── troubleshooting.md          # Universal troubleshooting (shared)
```

## File Responsibilities

### Main SKILL.md (Universal)
- **Keep**: Overview, Architecture, When to Use, Core Concepts
- **Keep**: Directory Layout, Storage Hierarchy
- **Keep**: Configuration Defaults (StoreConfig, DataSetConfig)
- **Keep**: Common Mistakes (universal)
- **Keep**: Naming Rules
- **Add**: Language Installation Summary (brief for each language)
- **Add**: Language Directory Links (with descriptions)
- **Remove**: Quick Start code examples (move to language dirs)
- **Remove**: API Quick Reference tables (move to language dirs)
- **Remove**: C FFI section (move to rust/ as it's part of Rust crate)

### Language Subdirectory SKILL.md (Entry Point)
Each language SKILL.md should contain:
- Language-specific installation instructions
- Quick start example
- Links to detailed documentation files
- Language-specific notes and conventions

### Language-Specific Files
- `quick-start.md`: Getting started guide with basic examples
- `api-reference.md`: Complete API signatures for that language
- `examples.md`: Feature scenarios and advanced examples

### troubleshooting.md (Shared)
- Remains at root level (universal errors apply to all languages)
- May add language-specific error notes in each language's guide

## Implementation Tasks

### Task 1: Create Directory Structure
Create the language subdirectories:
- `skills/timslite-guide/rust/`
- `skills/timslite-guide/python/`
- `skills/timslite-guide/nodejs/`
- `skills/timslite-guide/java/`

### Task 2: Extract Rust Content
**Source**: Current SKILL.md + scenarios.md + api-reference.md

**Create**:
1. `rust/SKILL.md` - Entry point with Rust installation and links
2. `rust/quick-start.md` - Extract from SKILL.md "Quick Start > Rust" section
3. `rust/api-reference.md` - Extract from api-reference.md (Rust signatures)
4. `rust/examples.md` - Extract from scenarios.md (Rust examples)

**Content to extract from SKILL.md**:
- Lines 85-108: Rust Quick Start example
- Lines 174-240: API Quick Reference tables (Rust method signatures)
- Lines 242-276: Configuration Defaults (keep in main, but also reference in rust/)

**Content to extract from scenarios.md**:
- All Rust code examples throughout the file

**Content to extract from api-reference.md**:
- Complete file (already Rust-focused)

### Task 3: Extract Python Content
**Source**: Current SKILL.md + scenarios.md + wrapper/python/README.md

**Create**:
1. `python/SKILL.md` - Entry point with Python installation and links
2. `python/quick-start.md` - Extract from SKILL.md "Quick Start > Python" section
3. `python/api-reference.md` - Python API signatures (adapt from Rust)
4. `python/examples.md` - Extract from scenarios.md (Python examples)

**Content to extract from SKILL.md**:
- Lines 110-156: Python Quick Start example

**Content to extract from scenarios.md**:
- Python code snippets throughout the file

**Additional source**: wrapper/python/README.md for installation and usage

### Task 4: Extract Node.js Content
**Source**: wrapper/nodejs/README.md + index.d.ts

**Create**:
1. `nodejs/SKILL.md` - Entry point with Node.js installation and links
2. `nodejs/quick-start.md` - Getting started guide
3. `nodejs/api-reference.md` - Node.js API signatures
4. `nodejs/examples.md` - Node.js code examples

**Source files to read**:
- `wrapper/nodejs/README.md` - Installation and usage
- `wrapper/nodejs/index.d.ts` - TypeScript definitions for API reference
- `wrapper/nodejs/tests/` - Test files for examples

### Task 5: Extract Java Content
**Source**: wrapper/java/README.md + wrapper/java/design.md

**Create**:
1. `java/SKILL.md` - Entry point with Java installation and links
2. `java/quick-start.md` - Getting started guide
3. `java/api-reference.md` - Java API signatures
4. `java/examples.md` - Java code examples

**Source files to read**:
- `wrapper/java/README.md` - Installation and usage
- `wrapper/java/design.md` - Design documentation
- `wrapper/java/src/main/java/io/github/snower/timslite/` - Java source for API

### Task 6: Update Main SKILL.md
**Modifications**:
1. Remove Quick Start code examples (lines 85-172)
2. Remove API Quick Reference tables (lines 174-240)
3. Add "Language Guides" section with links to subdirectories
4. Add brief installation summary for each language
5. Update "Detailed References" section to include language directories
6. Keep: Overview, Architecture, When to Use, Core Concepts, Configuration, Common Mistakes, Naming Rules

**New structure for main SKILL.md**:
```markdown
# timslite Developer Guide

## Overview
[Keep existing content]

## When to Use
[Keep existing content]

## Architecture
[Keep existing content]

## Core Concepts
[Keep existing content - may consolidate from other sections]

## Language Guides

### Rust
- Installation: Add to Cargo.toml
- [Quick Start](rust/quick-start.md)
- [API Reference](rust/api-reference.md)
- [Examples](rust/examples.md)

### Python
- Installation: pip install timslite
- [Quick Start](python/quick-start.md)
- [API Reference](python/api-reference.md)
- [Examples](python/examples.md)

### Node.js
- Installation: npm install timslite
- [Quick Start](nodejs/quick-start.md)
- [API Reference](nodejs/api-reference.md)
- [Examples](nodejs/examples.md)

### Java
- Installation: Maven dependency
- [Quick Start](java/quick-start.md)
- [API Reference](java/api-reference.md)
- [Examples](java/examples.md)

## Configuration
[Keep existing StoreConfig and DataSetConfig tables]

## Common Mistakes
[Keep existing content]

## Naming Rules
[Keep existing content]

## Troubleshooting
[Link to troubleshooting.md]

## Detailed References
[Links to language-specific documentation]
```

### Task 7: Review and Validate
- Verify all links work correctly
- Ensure no content is lost
- Check that language-specific content is properly separated
- Validate that universal content remains in main SKILL.md

## Testing

After implementation:
1. Verify all markdown links resolve correctly
2. Check that each language directory has complete documentation
3. Ensure the main SKILL.md provides clear navigation
4. Validate that no code examples are duplicated across languages

## Success Criteria

- [ ] Main SKILL.md contains only universal content
- [ ] Each language has its own directory with SKILL.md entry point
- [ ] Language-specific content is fully extracted
- [ ] All links work correctly
- [ ] No content is lost in the migration
- [ ] Navigation is clear and intuitive