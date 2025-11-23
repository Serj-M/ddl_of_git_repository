CREATE TABLE public.repositories (
	id serial4 NOT NULL,
	"name" text NOT NULL,
	url text NOT NULL,
	default_branch text NOT NULL,
	created_at timestamptz NOT NULL,
	CONSTRAINT pk_repositories PRIMARY KEY (id)
);


CREATE TABLE public.issues (
	id serial4 NOT NULL,
	repo_id int4 NOT NULL,
	title text NULL,
	description text NULL,
	issue_created_at timestamptz NULL,
	issue_closed_at timestamptz NULL,
	created_at timestamptz NOT NULL,
	url text NULL,
	issue_id text NULL,
	task_ids _int4 NULL,
	CONSTRAINT pk_issues PRIMARY KEY (id),
	CONSTRAINT fk_issues_repo_id_repositories FOREIGN KEY (repo_id) REFERENCES public.repositories(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX ix_issues_issue_id_unique ON public.issues USING btree (issue_id) WHERE (issue_id IS NOT NULL);


CREATE TABLE public.pull_requests (
	id serial4 NOT NULL,
	repo_id int4 NOT NULL,
	"number" int4 NOT NULL,
	title text NULL,
	state varchar(20) NULL,
	pr_created_at timestamptz NULL,
	pr_merged_at timestamptz NULL,
	author text NULL,
	url text NULL,
	created_at timestamptz NOT NULL,
	issue_ids _int4 NULL,
	CONSTRAINT pk_pull_requests PRIMARY KEY (id),
	CONSTRAINT fk_pull_requests_repo_id_repositories FOREIGN KEY (repo_id) REFERENCES public.repositories(id) ON DELETE CASCADE
);


CREATE TABLE public.commits (
	id varchar NOT NULL,
	repo_id int4 NOT NULL,
	hash varchar(40) NOT NULL,
	parent_hashes _text NULL,
	author_name text NULL,
	author_email text NULL,
	commit_created_at timestamptz NULL,
	message text NULL,
	pull_request_id int4 NULL,
	created_at timestamptz NOT NULL,
	CONSTRAINT pk_commits PRIMARY KEY (id),
	CONSTRAINT fk_commits_pull_request_id_pull_requests FOREIGN KEY (pull_request_id) REFERENCES public.pull_requests(id) ON DELETE CASCADE,
	CONSTRAINT fk_commits_repo_id_repositories FOREIGN KEY (repo_id) REFERENCES public.repositories(id) ON DELETE CASCADE
);


CREATE TABLE public.file_changes (
	id serial4 NOT NULL,
	repo_id int4 NOT NULL,
	commit_id varchar NULL,
	file_path text NULL,
	old_path text NULL,
	change_type varchar(20) NOT NULL,
	diff text NULL,
	created_at timestamptz NOT NULL,
	CONSTRAINT pk_file_changes PRIMARY KEY (id),
	CONSTRAINT uq_file_changes_repo_file_commit UNIQUE (repo_id, file_path, commit_id),
	CONSTRAINT fk_file_changes_commit_id_commits FOREIGN KEY (commit_id) REFERENCES public.commits(id) ON DELETE CASCADE,
	CONSTRAINT fk_file_changes_repo_id_repositories FOREIGN KEY (repo_id) REFERENCES public.repositories(id) ON DELETE CASCADE
);
