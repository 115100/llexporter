package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/spf13/pflag"
)

type Link struct {
	UID  string    `json:"uid"`
	Path string    `json:"path"`
	Name string    `json:"name"`
	Ext  string    `json:"ext"`
	MIME string    `json:"mime"`
	Date time.Time `json:"date"`
}

type Thumbnail struct {
	UID    string `json:"uid"`
	Data   []byte `json:"data"`
	MIME   string `json:"mime"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

func main() {
	if err := run(); err != nil {
		slog.Error("failed to run", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	var dump, restore, verbose bool
	pflag.BoolVarP(&dump, "dump", "d", false, "dump from MySQL to json files")
	pflag.BoolVarP(&restore, "restore", "r", false, "restore from json files to PostgreSQL")
	pflag.BoolVarP(&verbose, "verbose", "v", false, "verbose log output")
	pflag.Parse()

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if (!dump && !restore) || (dump && restore) {
		return errors.New("exactly one of [--dump, --restore] required")
	}

	if dump {
		slog.Debug("dumping MySQL records to JSON")
		if err := dumpMySQL(ctx); err != nil {
			return err
		}
	}
	if restore {
		slog.Debug("restoring JSON records to PostgreSQL")
		if err := restorePostgreSQL(ctx); err != nil {
			return err
		}
	}

	return nil
}

func dumpMySQL(ctx context.Context) error {
	db, err := sql.Open("mysql", "root@unix(/var/run/mysqld/mysqld.sock)/loadlink?parseTime=true")
	if err != nil {
		return err
	}
	defer db.Close()

	if err := os.Mkdir("links", 0755); err != nil && !os.IsExist(err) {
		return err
	}
	lr, err := db.QueryContext(ctx, "SELECT uid, path, name, ext, mime, date FROM links;")
	if err != nil {
		return err
	}
	defer lr.Close()

	for lr.Next() {
		l := new(Link)
		if err := lr.Scan(&l.UID, &l.Path, &l.Name, &l.Ext, &l.MIME, &l.Date); err != nil {
			return err
		}
		slog.Debug("dumping link", "uid", l.UID)
		w, err := os.Create(fmt.Sprintf("links/%s.json", l.UID))
		if err != nil {
			return err
		}
		if err := json.NewEncoder(w).Encode(l); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	if lr.Err() != nil {
		return lr.Err()
	}

	if err := os.Mkdir("thumbnails", 0755); err != nil && !os.IsExist(err) {
		if err != os.ErrExist {
			return err
		}
	}
	tr, err := db.QueryContext(ctx, "SELECT uid, data, mime, width, height FROM thumbnails;")
	if err != nil {
		return err
	}
	defer tr.Close()

	for tr.Next() {
		t := new(Thumbnail)
		if err := tr.Scan(&t.UID, &t.Data, &t.MIME, &t.Width, &t.Height); err != nil {
			return err
		}
		slog.Debug("dumping thumbnail", "uid", t.UID)
		w, err := os.Create(fmt.Sprintf("thumbnails/%s.json", t.UID))
		if err != nil {
			return err
		}
		if err := json.NewEncoder(w).Encode(t); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	if tr.Err() != nil {
		return tr.Err()
	}

	return nil
}

func restorePostgreSQL(ctx context.Context) error {
	db, err := sql.Open("pgx", "host=/run/postgresql database=loadlink")
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := filepath.WalkDir(
		"links",
		func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			slog.Debug("restoring link record", "path", path)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			l := new(Link)
			if err := json.NewDecoder(f).Decode(l); err != nil {
				return err
			}

			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO links (uid, path, name, ext, mime, date) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
				l.UID,
				l.Path,
				l.Name,
				l.Ext,
				l.MIME,
				l.Date,
			); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	if err := filepath.WalkDir(
		"thumbnails",
		func(path string, info fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}

			slog.Debug("restoring thumbnail record", "path", path)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			t := new(Thumbnail)
			if err := json.NewDecoder(f).Decode(t); err != nil {
				return err
			}

			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO thumbnails (uid, data, mime, width, height) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
				t.UID,
				t.Data,
				t.MIME,
				t.Width,
				t.Height,
			); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	return tx.Commit()
}
