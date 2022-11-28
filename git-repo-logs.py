#!/usr/bin/env python

import argparse
import logging
import os
from multiprocessing import Manager, Pool, Value
import subprocess
import sys
import time


from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from tqdm import tqdm


logger = logging.getLogger("debug.log")
logger.setLevel(logging.INFO)

# everything gets logged to github.log
fh = logging.FileHandler("debug.log")
# warning and finer log levels get logged to stdout
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)

logger.addHandler(fh)
logger.addHandler(ch)


def git_clone(org, repo):
    start = time.time()
    os.system(
        f"git clone --bare -q https://x-access-token:{os.environ['GITHUB_TOKEN']}@{os.environ['GITHUB_HOST']}/{org}/{repo}.git repos/{org}/{repo} > /dev/null 2>&1"
    )
    end = time.time() - start
    logger.info(f"[{org}/{repo}] - clone completed in {end:.2f} seconds")
    return end


def git_log(org, repo):
    start = time.time()
    os.system(
        f"git -C repos/{org}/{repo} log --all --pretty=format:'{org},{repo},%H,%ct,%an,%ae,%S,%s' > logs/{org}/{repo}/git.log.csv 2>/dev/null"
    )
    end = time.time() - start
    logger.info(f"[{org}/{repo}] - log completed in {end:.2f} seconds")
    return end


def gql_client(gql, gql_params):
    transport = RequestsHTTPTransport(
        url=os.environ["GITHUB_GRAPHQL_API"],
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}"},
        use_json=True,
    )
    client = Client(transport=transport)
    return client.execute(gql, variable_values=gql_params)


# paginate through orgs, add to data
def gql_orgs(enterprise, cursor=None):
    global api_calls
    api_calls.value += 1
    params = {"enterprise": enterprise, "cursor": cursor}
    # query enterprise for orgs
    org_query = gql(
        """
        query getOrgs($enterprise: String!, $cursor: String) {
            enterprise(slug: $enterprise) {
                organizations(first: 100, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    nodes {
                        login
                    }
                }
            }
        }
    """
    )
    response = gql_client(org_query, params)
    for org in response["enterprise"]["organizations"]["nodes"]:
        data[org["login"]] = []

    page = response["enterprise"]["organizations"]["pageInfo"]
    if page["hasNextPage"] is True:
        gql_orgs(enterprise, page["endCursor"])


def shell_cmd(cmd):
    ps = subprocess.Popen(
        cmd, text=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    output = ps.communicate()[0].strip()
    return output


def init_rtw_worker(api_calls):
    global repo_api_calls
    repo_api_calls = api_calls


class RepositoryTraversalWorker(object):
    def __init__(self, mutex, data):
        self.mutex = mutex
        self.data = data

    def __call__(self, org):
        self.gql_repos(org)

    # paginate through repos, add to data
    def gql_repos(self, org, cursor=None):
        with self.mutex:
            repo_api_calls.value += 1

        params = {"org": org, "cursor": cursor}
        # query orgs for repos
        repo_query = gql(
            """
            query getRepos($org: String!, $cursor: String) {
                organization(login: $org) {
                    repositories(first: 100, after: $cursor) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            name
                            isLocked
                            sshUrl
                        }
                    }
                }
            }
        """
        )
        response = gql_client(repo_query, params)
        for repo in response["organization"]["repositories"]["nodes"]:
            if repo["isLocked"]:
                logger.info(f"[{org}/{repo['name']}] - skipped - locked")
            elif (
                repo["sshUrl"]
                != f"git@{os.environ['GITHUB_HOST']}:{org}/{repo['name']}.git"
            ):
                logger.info(f"[{org}/{repo['name']}] - skipped - requires SSH CA")
            else:
                with self.mutex:
                    self.data[org] += [repo["name"]]

        page = response["organization"]["repositories"]["pageInfo"]
        if page["hasNextPage"] is True:
            self.gql_repos(org, page["endCursor"])


class RepositoryCloneWorker(object):
    def __init__(self, mutex):
        self.mutex = mutex

    def __call__(self, work_args):
        self.work(work_args)

    def work(self, work_args):
        org, repo = work_args
        os.system(f"mkdir -p logs/{org}/{repo}")
        clone = git_clone(org, repo)
        log = git_log(org, repo)
        os.system(f"rm -rf repos/{org}/{repo}")
        logger.info(f"[{org}/{repo}] - finished in {(clone + log):.2f} seconds")


if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(
        description="Obtain git logs for all org repos in a single enterprise",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    optional = parser._action_groups.pop()
    required = parser.add_argument_group("required arguments")

    required.add_argument(
        "-e",
        "--enterprise",
        type=str,
        help="GitHub Enterprise name/slug",
        required=True,
    )

    optional.add_argument(
        "-n",
        "--host",
        nargs="?",
        help="GitHub Enterprise hostname (domain.tld)\ngets/sets GITHUB_HOST env var",
    )

    optional.add_argument(
        "-t",
        "--token",
        nargs="?",
        type=str,
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub Enterprise admin PAT token\ngets/sets GITHUB_TOKEN env var",
    )
    optional.add_argument(
        "-c",
        "--cloning_processes",
        nargs="?",
        type=int,
        default=4,
        help="number of processes to use when cloning",
    )
    optional.add_argument(
        "-a",
        "--api_processes",
        nargs="?",
        type=int,
        default=1,
        help="""number of processes to use when traversing repos via the api
NOTE: keep this low to avoid rate limiting""",
    )

    parser._action_groups.append(optional)  # added this line
    args = parser.parse_args()

    if args.token is None:
        print("GitHub Enterprise admin PAT token not provided")
        sys.exit(1)

    if args.host:
        os.environ["GITHUB_GRAPHQL_API"] = f"https://{args.host}/api/graphql"
        os.environ["GITHUB_HOST"] = args.host
    else:
        os.environ["GITHUB_GRAPHQL_API"] = "https://api.github.com/graphql"
        os.environ["GITHUB_HOST"] = "github.com"

    os.environ["GITHUB_TOKEN"] = args.token

    logger.info("\n\n\n\n./git-logs.py running initiated!")

    print(
        f"""
       __ __   __          __
.-----|__|  |_|  |--.--.--|  |--.
|  _  |  |   _|     |  |  |  _  |
|___  |__|____|__|__|_____|_____|
|_____|
Enterprise git repo logs collector
"""
    )

    print(f"- Github host is set to: {os.environ['GITHUB_HOST']}")
    print(f"- Github graphql endpoint is set to: {os.environ['GITHUB_GRAPHQL_API']}")
    os.system("mkdir -p repos")

    # set up data structure
    manager = Manager()
    mutex = manager.Lock()
    data = manager.dict()
    api_calls = Value("i", 0)

    # query enterprise for orgs
    org_start_time = time.time()
    gql_orgs(args.enterprise)
    org_end_time = time.time() - org_start_time

    logger.warning(
        f"- Querying enterprise for organizations\n"
        f"- Found {len(data)} organizations in {args.enterprise}, took {org_end_time:.2f} seconds\n"
        f"- Querying organizations for repositories"
    )

    # query orgs for repos
    repo_start_time = time.time()
    # traverse repositories, visualize using tqdm
    # can also use multiple processes (and likely hits ratelimits)
    worker = RepositoryTraversalWorker(mutex, data)
    p = Pool(args.api_processes, initializer=init_rtw_worker, initargs=(api_calls,))
    for _ in tqdm(p.imap_unordered(worker, list(data.keys())), total=len(data)):
        pass
    repo_end_time = time.time() - repo_start_time

    # count repos
    repo_count = sum([len(org) for org in data.values()])
    logger.warning(
        f"- Found {repo_count} repositories in {args.enterprise}, took {repo_end_time:.2f} seconds"
    )
    logger.warning(
        f"- {api_calls.value} total api requests made in {(org_end_time + repo_end_time):.2f} seconds"
    )

    repos = [(org, repo) for org, repos in data.items() for repo in repos]

    # scale up clones/processes, use tqdm to visualize
    worker = RepositoryCloneWorker(mutex)
    p = Pool(args.cloning_processes)
    for _ in tqdm(p.imap_unordered(worker, repos), total=len(repos)):
        pass

    # post process
    empty = shell_cmd("find logs/ -size 0 | wc -l")
    locked = shell_cmd("cat debug.log | grep -i 'skipped - locked' | wc -l")
    ssh_ca = shell_cmd("cat debug.log | grep -i 'skipped - requires SSH CA' | wc -l")
    total_commits = shell_cmd(
        "find logs -type f -name '*.csv' -print0 | xargs -0 cat | wc -l"
    )

    logger.warning(
        f"- Found {total_commits} total commits\n"
        f"- Found {empty} empty repositories with zero commits\n"
        f"- Skipped {locked} repositories due to being locked\n"
        f"- Skipped {ssh_ca} repositories that require SSH CA\n"
        f"- took {time.time() - start:.2f} seconds\n"
    )
