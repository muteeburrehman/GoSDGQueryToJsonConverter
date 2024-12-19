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
	AND     []interface{} `json:"AND,omitempty"`
	OR      []interface{} `json:"OR,omitempty"`
	AND_NOT []interface{} `json:"AND_NOT,omitempty"`
	Field   *Field        `json:"field,omitempty"`
}

func isEmptyQuery(query *ParsedQuery) bool {
	return query == nil ||
		(query.Field == nil &&
			(query.AND == nil || len(query.AND) == 0) &&
			(query.OR == nil || len(query.OR) == 0) &&
			(query.AND_NOT == nil || len(query.AND_NOT) == 0))
}

func cleanupQuery(query *ParsedQuery) *ParsedQuery {
	if query == nil {
		return nil
	}

	if query.AND != nil {
		var cleanAND []interface{}
		for _, item := range query.AND {
			if subQuery, ok := item.(ParsedQuery); ok {
				cleaned := cleanupQuery(&subQuery)
				if !isEmptyQuery(cleaned) {
					cleanAND = append(cleanAND, *cleaned)
				}
			} else if subQuery, ok := item.(*ParsedQuery); ok {
				cleaned := cleanupQuery(subQuery)
				if !isEmptyQuery(cleaned) {
					cleanAND = append(cleanAND, *cleaned)
				}
			} else if field, ok := item.(Field); ok {
				cleanAND = append(cleanAND, ParsedQuery{Field: &field})
			}
		}
		if len(cleanAND) > 0 {
			query.AND = cleanAND
		} else {
			query.AND = nil
		}
	}

	if query.OR != nil {
		var cleanOR []interface{}
		for _, item := range query.OR {
			if subQuery, ok := item.(ParsedQuery); ok {
				cleaned := cleanupQuery(&subQuery)
				if !isEmptyQuery(cleaned) {
					cleanOR = append(cleanOR, *cleaned)
				}
			} else if subQuery, ok := item.(*ParsedQuery); ok {
				cleaned := cleanupQuery(subQuery)
				if !isEmptyQuery(cleaned) {
					cleanOR = append(cleanOR, *cleaned)
				}
			} else if field, ok := item.(Field); ok {
				cleanOR = append(cleanOR, ParsedQuery{Field: &field})
			}
		}
		if len(cleanOR) > 0 {
			query.OR = cleanOR
		} else {
			query.OR = nil
		}
	}

	if query.AND_NOT != nil {
		var cleanANDNOT []interface{}
		for _, item := range query.AND_NOT {
			if subQuery, ok := item.(ParsedQuery); ok {
				cleaned := cleanupQuery(&subQuery)
				if !isEmptyQuery(cleaned) {
					cleanANDNOT = append(cleanANDNOT, *cleaned)
				}
			} else if subQuery, ok := item.(*ParsedQuery); ok {
				cleaned := cleanupQuery(subQuery)
				if !isEmptyQuery(cleaned) {
					cleanANDNOT = append(cleanANDNOT, *cleaned)
				}
			} else if field, ok := item.(Field); ok {
				cleanANDNOT = append(cleanANDNOT, ParsedQuery{Field: &field})
			}
		}
		if len(cleanANDNOT) > 0 {
			query.AND_NOT = cleanANDNOT
		} else {
			query.AND_NOT = nil
		}
	}

	return query
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

	re := regexp.MustCompile(`(TITLE-ABS-KEY|TITLE-ABS|TITLE|AUTHKEY)\s*\("([^"]+)"\)|OR|AND_NOT|AND|\(|\)|"([^"]+)"`)

	matches := re.FindAllStringSubmatchIndex(query, -1)

	for matchIndex, match := range matches {
		fullMatch := query[match[0]:match[1]]
		fullMatch = strings.TrimSpace(fullMatch)

		var currentToken Token

		switch {
		case fullMatch == "OR":
			currentToken = Token{Type: TokenOR, Value: fullMatch}
		case fullMatch == "AND":
			currentToken = Token{Type: TokenAND, Value: fullMatch}
		case fullMatch == "AND_NOT":
			currentToken = Token{Type: TokenANDNOT, Value: fullMatch}
		case fullMatch == "(":
			currentToken = Token{Type: TokenOpenParen, Value: fullMatch}
		case fullMatch == ")":
			currentToken = Token{Type: TokenCloseParen, Value: fullMatch}
		default:
			if strings.Contains(fullMatch, "(") {
				re := regexp.MustCompile(`(TITLE-ABS-KEY|TITLE-ABS|TITLE|AUTHKEY)\s*\("([^"]+)"\)`)
				subMatches := re.FindStringSubmatch(fullMatch)
				if len(subMatches) == 3 {
					fieldType := subMatches[1]
					value := subMatches[2]
					currentToken = Token{
						Type:  TokenField,
						Value: fmt.Sprintf("%s:%s", fieldType, value),
					}
				}
			} else if strings.HasPrefix(fullMatch, `"`) && strings.HasSuffix(fullMatch, `"`) {
				value := strings.Trim(fullMatch, `"`)
				currentToken = Token{
					Type:  TokenField,
					Value: fmt.Sprintf("ANY:%s", value),
				}
			}
		}

		if currentToken.Type != 0 || currentToken.Value != "" {
			tokens = append(tokens, currentToken)

			if matchIndex < len(matches)-1 && currentToken.Type == TokenField {
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
	var stack []*ParsedQuery
	var currentGroup *ParsedQuery
	currentOperator := TokenAND

	for _, token := range tokens {
		switch token.Type {
		case TokenField:
			parts := strings.Split(token.Value, ":")
			if len(parts) != 2 {
				continue
			}

			field := &Field{
				Field: parts[0],
				Value: parts[1],
			}

			if currentGroup == nil {
				currentGroup = &ParsedQuery{}
			}

			newQuery := ParsedQuery{Field: field}

			switch currentOperator {
			case TokenOR:
				if currentGroup.OR == nil {
					currentGroup.OR = []interface{}{}
				}
				currentGroup.OR = append(currentGroup.OR, newQuery)
			case TokenANDNOT:
				if currentGroup.AND_NOT == nil {
					currentGroup.AND_NOT = []interface{}{}
				}
				currentGroup.AND_NOT = append(currentGroup.AND_NOT, newQuery)
			default:
				if currentGroup.AND == nil {
					currentGroup.AND = []interface{}{}
				}
				currentGroup.AND = append(currentGroup.AND, newQuery)
			}

		case TokenOR, TokenAND, TokenANDNOT:
			currentOperator = token.Type

		case TokenOpenParen:
			newGroup := &ParsedQuery{}
			if currentGroup != nil {
				stack = append(stack, currentGroup)
			}
			currentGroup = newGroup

		case TokenCloseParen:
			if len(stack) > 0 && currentGroup != nil {
				lastIndex := len(stack) - 1
				parent := stack[lastIndex]
				stack = stack[:lastIndex]

				cleaned := cleanupQuery(currentGroup)
				if !isEmptyQuery(cleaned) {
					switch currentOperator {
					case TokenOR:
						if parent.OR == nil {
							parent.OR = []interface{}{}
						}
						parent.OR = append(parent.OR, *cleaned)
					case TokenANDNOT:
						if parent.AND_NOT == nil {
							parent.AND_NOT = []interface{}{}
						}
						parent.AND_NOT = append(parent.AND_NOT, *cleaned)
					default:
						if parent.AND == nil {
							parent.AND = []interface{}{}
						}
						parent.AND = append(parent.AND, *cleaned)
					}
				}
				currentGroup = parent
			}
		}
	}

	return cleanupQuery(currentGroup)
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

	var allQueries []interface{}
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
			allQueries = append(allQueries, parsedQuery)
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
