package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	TokenField = iota
	TokenOR
	TokenAND
	TokenANDNOT
	TokenOpenParen
	TokenCloseParen
)

type Token struct {
	Type  int
	Value string
}

type Field struct {
	Field string `json:"field"`
	Value string `json:"value"`
}

type ParsedQuery struct {
	AND     []ParsedQuery `json:"AND,omitempty"`
	OR      []ParsedQuery `json:"OR,omitempty"`
	AND_NOT []ParsedQuery `json:"AND_NOT,omitempty"`
	Field   *Field        `json:"field,omitempty"`
}

func isEmptyQuery(query *ParsedQuery) bool {
	return query == nil ||
		(query.Field == nil &&
			len(query.AND) == 0 &&
			len(query.OR) == 0 &&
			len(query.AND_NOT) == 0)
}

func cleanupQuery(query *ParsedQuery) *ParsedQuery {
	if query == nil {
		return nil
	}

	// Create a new query to hold cleaned data
	cleaned := &ParsedQuery{}

	// Clean AND queries
	if len(query.AND) > 0 {
		for _, subQuery := range query.AND {
			if cleanedSub := cleanupQuery(&subQuery); !isEmptyQuery(cleanedSub) {
				cleaned.AND = append(cleaned.AND, *cleanedSub)
			}
		}
	}

	// Clean OR queries
	if len(query.OR) > 0 {
		for _, subQuery := range query.OR {
			if cleanedSub := cleanupQuery(&subQuery); !isEmptyQuery(cleanedSub) {
				cleaned.OR = append(cleaned.OR, *cleanedSub)
			}
		}
	}

	// Clean AND_NOT queries
	if len(query.AND_NOT) > 0 {
		for _, subQuery := range query.AND_NOT {
			if cleanedSub := cleanupQuery(&subQuery); !isEmptyQuery(cleanedSub) {
				cleaned.AND_NOT = append(cleaned.AND_NOT, *cleanedSub)
			}
		}
	}

	// Copy field if it exists
	if query.Field != nil {
		cleaned.Field = query.Field
	}

	// If the cleaned query is empty, return nil
	if isEmptyQuery(cleaned) {
		return nil
	}

	// Simplify single-item arrays
	if len(cleaned.AND) == 1 && len(cleaned.OR) == 0 && len(cleaned.AND_NOT) == 0 && cleaned.Field == nil {
		return cleanupQuery(&cleaned.AND[0])
	}
	if len(cleaned.OR) == 1 && len(cleaned.AND) == 0 && len(cleaned.AND_NOT) == 0 && cleaned.Field == nil {
		return cleanupQuery(&cleaned.OR[0])
	}
	if len(cleaned.AND_NOT) == 1 && len(cleaned.AND) == 0 && len(cleaned.OR) == 0 && cleaned.Field == nil {
		return cleanupQuery(&cleaned.AND_NOT[0])
	}

	return cleaned
}

func tokenize(query string) []Token {
	var tokens []Token

	// Balance parentheses
	openCount := strings.Count(query, "(")
	closeCount := strings.Count(query, ")")
	if openCount > closeCount {
		query += strings.Repeat(")", openCount-closeCount)
	}

	query = strings.ReplaceAll(query, "( ", "(")
	query = strings.ReplaceAll(query, " )", ")")
	query = strings.ReplaceAll(query, " AND NOT ", " AND_NOT ")
	query = strings.ReplaceAll(query, " AND ", " AND ")
	query = strings.ReplaceAll(query, " OR ", " OR ")

	re := regexp.MustCompile(`(TITLE-ABS-KEY|TITLE-ABS|TITLE|AUTHKEY)\s*\("([^"]+?)"\)|OR|AND_NOT|AND|\(|\)|"([^"]+?)"`)
	matches := re.FindAllStringSubmatchIndex(query, -1)

	for matchIndex, match := range matches {
		fullMatch := strings.TrimSpace(query[match[0]:match[1]])
		var token Token

		switch {
		case fullMatch == "OR":
			token = Token{Type: TokenOR, Value: fullMatch}
		case fullMatch == "AND":
			token = Token{Type: TokenAND, Value: fullMatch}
		case fullMatch == "AND_NOT":
			token = Token{Type: TokenANDNOT, Value: fullMatch}
		case fullMatch == "(":
			token = Token{Type: TokenOpenParen, Value: fullMatch}
		case fullMatch == ")":
			token = Token{Type: TokenCloseParen, Value: fullMatch}
		default:
			if strings.Contains(fullMatch, "(") {
				re := regexp.MustCompile(`(TITLE-ABS-KEY|TITLE-ABS|TITLE|AUTHKEY)\s*\("((?:[^"\\]|\\.)+)"\)`)
				if subMatches := re.FindStringSubmatch(fullMatch); len(subMatches) == 3 {
					token = Token{
						Type:  TokenField,
						Value: fmt.Sprintf("%s:%s", subMatches[1], subMatches[2]),
					}
				}
			} else if strings.HasPrefix(fullMatch, `"`) && strings.HasSuffix(fullMatch, `"`) {
				token = Token{
					Type:  TokenField,
					Value: fmt.Sprintf("ANY:%s", strings.Trim(fullMatch, `"`)),
				}
			}
		}

		if token.Type != 0 || token.Value != "" {
			tokens = append(tokens, token)

			// Add implicit AND operators
			if matchIndex < len(matches)-1 && token.Type == TokenField {
				nextStart := matches[matchIndex+1][0]
				between := strings.TrimSpace(query[match[1]:nextStart])
				if between == "" && len(tokens) > 1 &&
					tokens[len(tokens)-2].Type != TokenOR &&
					tokens[len(tokens)-2].Type != TokenAND &&
					tokens[len(tokens)-2].Type != TokenANDNOT {
					tokens = append(tokens, Token{Type: TokenAND, Value: "AND"})
				}
			}
		}
	}
	return tokens
}

func parseTokens(tokens []Token) *ParsedQuery {
	type StackItem struct {
		query    *ParsedQuery
		operator int
	}

	var stack []StackItem
	current := StackItem{query: &ParsedQuery{}, operator: TokenAND}

	for _, token := range tokens {
		switch token.Type {
		case TokenField:
			parts := strings.Split(token.Value, ":")
			if len(parts) != 2 {
				continue
			}

			fieldQuery := ParsedQuery{
				Field: &Field{
					Field: parts[0],
					Value: parts[1],
				},
			}

			switch current.operator {
			case TokenAND:
				current.query.AND = append(current.query.AND, fieldQuery)
			case TokenOR:
				current.query.OR = append(current.query.OR, fieldQuery)
			case TokenANDNOT:
				current.query.AND_NOT = append(current.query.AND_NOT, fieldQuery)
			}

		case TokenOR, TokenAND, TokenANDNOT:
			current.operator = token.Type

		case TokenOpenParen:
			stack = append(stack, current)
			current = StackItem{query: &ParsedQuery{}, operator: TokenAND}

		case TokenCloseParen:
			if len(stack) == 0 {
				continue
			}

			if cleaned := cleanupQuery(current.query); !isEmptyQuery(cleaned) {
				parent := stack[len(stack)-1]
				switch parent.operator {
				case TokenAND:
					parent.query.AND = append(parent.query.AND, *cleaned)
				case TokenOR:
					parent.query.OR = append(parent.query.OR, *cleaned)
				case TokenANDNOT:
					parent.query.AND_NOT = append(parent.query.AND_NOT, *cleaned)
				}
			}

			current = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
		}
	}

	return cleanupQuery(current.query)
}

func processQuery(query string) (*ParsedQuery, error) {
	tokens := tokenize(query)
	parsedQuery := parseTokens(tokens)
	if isEmptyQuery(parsedQuery) {
		return nil, fmt.Errorf("query parsed to empty structure")
	}
	return parsedQuery, nil
}

func processFile(inputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %v", err)
	}
	defer file.Close()

	outputPath := strings.TrimSuffix(inputPath, filepath.Ext(inputPath)) + ".json"
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outputFile.Close()

	var allQueries []ParsedQuery
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parsedQuery, err := processQuery(line)
		if err != nil {
			fmt.Printf("Warning: Error processing line %d: %v\n", lineNumber, err)
			continue
		}

		if !isEmptyQuery(parsedQuery) {
			allQueries = append(allQueries, *parsedQuery)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %v", err)
	}

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(allQueries); err != nil {
		return fmt.Errorf("error writing JSON to file: %v", err)
	}

	fmt.Printf("Successfully processed %d queries. Output written to: %s\n", len(allQueries), outputPath)
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <input_file.txt>")
		os.Exit(1)
	}

	inputFile := os.Args[1]
	if err := processFile(inputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
