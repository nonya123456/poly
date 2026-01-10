package polymarket

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const GammaBaseURL = "https://gamma-api.polymarket.com"

var (
	ErrUnexpectedStatusCode = errors.New("unexpected status code")
	ErrMarketNotFound       = errors.New("market not found")
)

type GammaService struct {
	httpClient *http.Client
}

func NewGammaService(baseURL string) *GammaService {
	return &GammaService{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type MarketResponse struct {
	ID           string          `json:"id"`
	Question     string          `json:"question"`
	Slug         string          `json:"slug"`
	Outcomes     json.RawMessage `json:"outcomes"`
	ClobTokenIDs json.RawMessage `json:"clobTokenIds"`
}

type Market struct {
	ID           string
	Question     string
	Slug         string
	Outcomes     []string
	ClobTokenIDs []string
}

func (s *GammaService) GetMarketBySlug(slug string) (Market, error) {
	params := url.Values{}
	params.Add("slug", slug)
	params.Add("limit", "1")

	reqURL := fmt.Sprintf("%s/markets?%s", GammaBaseURL, params.Encode())
	resp, err := s.httpClient.Get(reqURL)
	if err != nil {
		return Market{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Market{}, ErrUnexpectedStatusCode
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Market{}, err
	}

	var marketsResp []MarketResponse
	if err := json.Unmarshal(body, &marketsResp); err != nil {
		return Market{}, err
	}
	if len(marketsResp) == 0 {
		return Market{}, ErrMarketNotFound
	}

	mr := marketsResp[0]

	var tokenIDsStr string
	if err := json.Unmarshal(mr.ClobTokenIDs, &tokenIDsStr); err != nil {
		return Market{}, fmt.Errorf("failed to parse token IDs string: %w", err)
	}

	var tokenIDs []string
	if err := json.Unmarshal([]byte(tokenIDsStr), &tokenIDs); err != nil {
		return Market{}, fmt.Errorf("failed to parse token IDs array: %w", err)
	}

	var outcomesStr string
	if err := json.Unmarshal(mr.Outcomes, &outcomesStr); err != nil {
		return Market{}, fmt.Errorf("failed to parse outcomes string: %w", err)
	}

	var outcomes []string
	if err := json.Unmarshal([]byte(outcomesStr), &outcomes); err != nil {
		return Market{}, fmt.Errorf("failed to parse outcomes array: %w", err)
	}

	return Market{
		ID:           mr.ID,
		Question:     mr.Question,
		Slug:         mr.Slug,
		Outcomes:     outcomes,
		ClobTokenIDs: tokenIDs,
	}, nil
}
