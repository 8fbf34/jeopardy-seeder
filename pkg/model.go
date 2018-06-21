package pkg

import "fmt"

type JeopardyEntry struct {
	Id		int		`json:"id"`
	Answer		string		`json:"answer"`
	Question	string		`json:"question"`
	Value		int		`json:"value"`
	Airdate		string		`json:"airdate"`
	CreatedAt	string		`json:"created_at"`
	UpdatedAt	string		`json:"updated_at"`
	CategoryId	int		`json:"category_id"`
	GameId		int		`json:"game_id"`
	InvalidCount	int		`json:"invalid_count"`
	Category	Category	`json:"category"`
}

type Category struct {
	Id		int	`json:"id"`
	Title		string	`json:"title"`
	CreatedAt	string	`json:"created_at"`
	UpdatedAt	string	`json:"updated_at"`
	CluesCount	int	`json:"count"`
}

func (j JeopardyEntry) String() string {
	return fmt.Sprintf("{id:%d, question:%s, answer:%s, category:%d}",
		j.Id,
		j.Question,
		j.Answer,
		j.CategoryId)
}

type Config struct {
	DbName				string	`json:"db_name"`
	Hostname			string	`json:"hostname"`
	Port					int			`json:"port"`
	Username			string	`json:"username"`
	Password			string	`json:"password"`
	SslMode				string	`json:"ssl_mode"`
	RestEndpoint	string	`json:"rest_endpoint"`
}
