package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/joho/godotenv"
)

// SyncOptions holds configuration parameters for the environment synchronization.
type SyncOptions struct {
	EnvPath    string
	ValuesPath string
	DryRun     bool
	CheckOnly  bool
}

func main() {
	envPath := flag.String("env", ".env", "Path to environment file")
	valuesPath := flag.String("values", filepath.Join("deployments", "helm", "values.yaml"), "Path to Helm values.yaml")
	dryRun := flag.Bool("dry-run", false, "Preview changes without writing to values.yaml")
	checkOnly := flag.Bool("check", false, "Verify if values.yaml is synchronized with env file (exits with code 1 if out of sync)")
	flag.Parse()

	opts := SyncOptions{
		EnvPath:    *envPath,
		ValuesPath: *valuesPath,
		DryRun:     *dryRun,
		CheckOnly:  *checkOnly,
	}

	if err := runSync(opts); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runSync(opts SyncOptions) error {
	// If the env file does not exist, log info and exit cleanly unless check-only is requested.
	if _, err := os.Stat(opts.EnvPath); os.IsNotExist(err) {
		fmt.Printf("[envsync] %s does not exist; skipping sync.\n", opts.EnvPath)
		return nil
	}

	envMap, err := godotenv.Read(opts.EnvPath)
	if err != nil {
		return fmt.Errorf("failed to read env file %s: %w", opts.EnvPath, err)
	}

	valuesBytes, err := os.ReadFile(opts.ValuesPath)
	if err != nil {
		return fmt.Errorf("failed to read helm values file %s: %w", opts.ValuesPath, err)
	}

	newValues, changed, changes, err := SyncContent(string(valuesBytes), envMap)
	if err != nil {
		return fmt.Errorf("failed to sync values: %w", err)
	}

	if !changed {
		fmt.Printf("[envsync] %s is already up to date with %s (0 changes).\n", opts.ValuesPath, opts.EnvPath)
		return nil
	}

	for _, c := range changes {
		fmt.Printf("[envsync] UPDATE %s\n", c)
	}

	if opts.CheckOnly {
		return fmt.Errorf("helm values file %s is out of sync with %s (%d changes pending)", opts.ValuesPath, opts.EnvPath, len(changes))
	}

	if opts.DryRun {
		fmt.Printf("[envsync] Dry run complete. %d updates would be written to %s.\n", len(changes), opts.ValuesPath)
		return nil
	}

	if err := os.WriteFile(opts.ValuesPath, []byte(newValues), 0644); err != nil {
		return fmt.Errorf("failed to write updated values to %s: %w", opts.ValuesPath, err)
	}

	fmt.Printf("[envsync] Successfully synchronized %d configuration values from %s to %s.\n", len(changes), opts.EnvPath, opts.ValuesPath)
	return nil
}

// SyncContent synchronizes target fields in Helm values content based on values in envMap.
// Preserves all comments, line feeds, indentation, and unrelated YAML blocks.
func SyncContent(valuesContent string, envMap map[string]string) (string, bool, []string, error) {
	lines := strings.Split(valuesContent, "\n")
	var changes []string
	currentSection := ""

	reSection := regexp.MustCompile(`^([a-zA-Z0-9_-]+):\s*$`)
	reIndent := regexp.MustCompile(`^(\s*)`)

	for i, line := range lines {
		trimmed := strings.TrimRight(line, "\r")

		// Detect top-level section headers (no leading whitespace)
		if m := reSection.FindStringSubmatch(trimmed); len(m) > 1 {
			currentSection = m[1]
			continue
		}

		// If a line is unindented and has a colon, update currentSection
		if !strings.HasPrefix(trimmed, " ") && !strings.HasPrefix(trimmed, "\t") && strings.Contains(trimmed, ":") {
			parts := strings.SplitN(trimmed, ":", 2)
			currentSection = strings.TrimSpace(parts[0])
		}

		indent := reIndent.FindString(trimmed)

		switch currentSection {
		case "env":
			if val, ok := envMap["HTTP_PORT"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "httpPort:") {
				targetLine := fmt.Sprintf(`%shttpPort: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("env.httpPort: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}
			if val, ok := envMap["GRPC_PORT"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "grpcPort:") {
				targetLine := fmt.Sprintf(`%sgrpcPort: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("env.grpcPort: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}
			if val, ok := envMap["LOG_LEVEL"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "logLevel:") {
				targetLine := fmt.Sprintf(`%slogLevel: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("env.logLevel: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}

		case "config":
			if val, ok := envMap["DATABASE_URL"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "databaseUrl:") {
				targetLine := fmt.Sprintf(`%sdatabaseUrl: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("config.databaseUrl: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}
			if val, ok := envMap["REDIS_ADDR"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "redisAddr:") {
				targetLine := fmt.Sprintf(`%sredisAddr: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("config.redisAddr: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}

		case "service":
			if val, ok := envMap["HTTP_PORT"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "httpPort:") {
				targetLine := fmt.Sprintf(`%shttpPort: %s`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("service.httpPort: %s -> %s", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}
			if val, ok := envMap["GRPC_PORT"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "grpcPort:") {
				targetLine := fmt.Sprintf(`%sgrpcPort: %s`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("service.grpcPort: %s -> %s", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}

		case "podAnnotations":
			if val, ok := envMap["HTTP_PORT"]; ok && strings.HasPrefix(strings.TrimSpace(trimmed), "prometheus.io/port:") {
				targetLine := fmt.Sprintf(`%sprometheus.io/port: %q`, indent, val)
				if trimmed != targetLine {
					changes = append(changes, fmt.Sprintf("podAnnotations[prometheus.io/port]: %s -> %q", strings.TrimSpace(trimmed), val))
					lines[i] = targetLine
				}
			}
		}
	}

	newContent := strings.Join(lines, "\n")
	return newContent, len(changes) > 0, changes, nil
}
