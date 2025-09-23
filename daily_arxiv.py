# -------------------------- 导入依赖库（按「标准库→第三方库」排序） --------------------------
# 标准库：文件操作、正则、JSON/XML解析、日志、命令行参数、日期
import os
import re
import json
import logging
import argparse
import datetime

# 第三方库：arXiv爬取、YAML配置、HTTP请求
import arxiv
import yaml
import requests


# -------------------------- 全局常量配置（集中管理，便于修改） --------------------------
# PapersWithCode API：用于获取论文对应的代码仓库
PAPERS_WITH_CODE_BASE_URL = "https://arxiv.paperswithcode.com/api/v0/papers/"
# GitHub搜索API：当PapersWithCode无代码时，备用搜索仓库
GITHUB_SEARCH_URL = "https://api.github.com/search/repositories"
# arXiv基础URL：用于拼接论文详情页链接
ARXIV_BASE_URL = "http://arxiv.org/"


# -------------------------- 日志配置（统一日志格式，便于调试） --------------------------
logging.basicConfig(
    format='[%(asctime)s %(levelname)s] %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S',
    level=logging.INFO  # 日志级别：INFO（普通信息）、ERROR（错误信息）
)


# -------------------------- 辅助工具函数（独立拆分，功能单一） --------------------------
def parse_filter_keywords(filters: list) -> str:
    """
    解析配置文件中的关键词过滤器，格式化为arXiv搜索支持的字符串
    规则：多词关键词加双引号，单词直接保留，用OR连接多个关键词
    
    Args:
        filters: 关键词列表，例如 ["SLAM", "3D reconstruction"]
    Returns:
        格式化后的搜索字符串，例如 '"SLAM" OR "3D reconstruction"'
    """
    formatted_filter = ""
    OR_SEPARATOR = " OR "  # 关键词间的OR逻辑
    QUOTE = '"'            # 多词关键词的引号包裹
    
    for idx, filter_word in enumerate(filters):
        # 多词关键词需要用双引号包裹，避免被拆分成单个词搜索
        if len(filter_word.split()) > 1:
            formatted_filter += f"{QUOTE}{filter_word}{QUOTE}"
        else:
            formatted_filter += filter_word
        
        # 除了最后一个关键词，后面都加OR分隔符
        if idx != len(filters) - 1:
            formatted_filter += OR_SEPARATOR
    
    return formatted_filter


def process_config_keywords(config: dict) -> dict:
    """
    处理配置文件中的关键词，将每个主题对应的过滤器格式化为搜索字符串
    
    Args:
        config: 原始配置字典，需包含 "keywords" 字段（例如 {"SLAM": {"filters": ["SLAM", "3D"]}}）
    Returns:
        新增 "formatted_keywords" 字段的配置字典，存储格式化后的搜索词
    """
    formatted_keywords = {}
    # 遍历每个主题（如"SLAM"）及其对应的过滤器列表
    for topic, topic_config in config["keywords"].items():
        formatted_keywords[topic] = parse_filter_keywords(topic_config["filters"])
    
    return formatted_keywords


def load_config(config_file_path: str) -> dict:
    """
    加载YAML配置文件，并处理关键词过滤器
    
    Args:
        config_file_path: 配置文件路径（如 "config.yaml"）
    Returns:
        完整配置字典，包含原始配置 + 格式化后的关键词（formatted_keywords字段）
    """
    # 读取YAML配置文件
    with open(config_file_path, "r", encoding="utf-8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    # 处理关键词过滤器，添加到配置中
    config["formatted_keywords"] = process_config_keywords(config)
    
    logging.info(f"成功加载配置：{config}")
    return config


def format_authors(authors: list, only_first_author: bool = False) -> str:
    """
    格式化论文作者列表，支持返回「第一作者」或「所有作者」
    
    Args:
        authors: arxiv库返回的作者对象列表
        only_first_author: 若为True，仅返回第一作者；否则返回所有作者（逗号分隔）
    Returns:
        格式化后的作者字符串，例如 "张三 et.al." 或 "张三, 李四, 王五"
    """
    # 转换作者对象为字符串（arxiv.Author对象的__str__为姓名）
    author_names = [str(author) for author in authors]
    
    if only_first_author:
        # 仅返回第一作者，后面加 "et.al." 表示等作者
        return f"{author_names[0]} et.al." if author_names else "Unknown Author"
    else:
        # 返回所有作者，用逗号分隔
        return ", ".join(author_names) if author_names else "Unknown Authors"


def sort_papers_by_id_desc(papers: dict) -> dict:
    """
    按论文ID倒序排序（论文ID含时间信息，倒序即最新的在前）
    
    Args:
        papers: 论文字典，key为论文ID（如 "2108.09112"），value为论文信息字符串
    Returns:
        按论文ID倒序排列后的新字典
    """
    sorted_paper_ids = sorted(papers.keys(), reverse=True)
    return {paper_id: papers[paper_id] for paper_id in sorted_paper_ids}


def search_github_code(query: str) -> str | None:
    """
    搜索GitHub仓库，获取与论文相关的代码链接（按星数排序取Top1）
    
    Args:
        query: 搜索关键词（如论文标题、论文ID）
    Returns:
        仓库HTML链接（若找到）；否则返回None
    """
    # 构造GitHub搜索参数：按星数降序，优先找高星仓库
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc"
    }
    
    try:
        # 发送GET请求到GitHub API
        response = requests.get(GITHUB_SEARCH_URL, params=params)
        response.raise_for_status()  # 若请求失败（如404/500），抛出异常
        search_results = response.json()
        
        # 若有搜索结果，返回第一个仓库的链接
        if search_results["total_count"] > 0:
            return search_results["items"][0]["html_url"]
        return None
    
    except Exception as e:
        logging.error(f"GitHub代码搜索失败（关键词：{query}），错误：{e}")
        return None


# -------------------------- 核心逻辑函数（论文爬取、数据更新、格式转换） --------------------------
def fetch_daily_arxiv_papers(topic: str, search_query: str, max_results: int = 2) -> tuple[dict, dict]:
    """
    从arXiv爬取指定主题的最新论文，获取论文基本信息及代码链接
    
    Args:
        topic: 论文主题（如 "SLAM"）
        search_query: 格式化后的arXiv搜索关键词
        max_results: 最多爬取的论文数量
    Returns:
        两个字典：
        1. markdown_table_data: 用于生成表格的论文数据（key=论文ID，value=Markdown表格行）
        2. markdown_list_data: 用于生成列表的论文数据（key=论文ID，value=Markdown列表项）
    """
    markdown_table_data = {}  # 用于README/GitPage的表格格式
    markdown_list_data = {}   # 用于微信推送的列表格式
    
    # 初始化arXiv搜索器：按提交日期排序（最新的在前）
    arxiv_searcher = arxiv.Search(
        query=search_query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate
    )
    
    # 遍历搜索结果，提取每篇论文的信息
    for paper in arxiv_searcher.results():
        # -------------------------- 1. 提取论文基础信息 --------------------------
        raw_paper_id = paper.get_short_id()  # 原始ID（含版本，如 "2108.09112v1"）
        paper_title = paper.title
        paper_arxiv_url = paper.entry_id  # 原始arXiv链接
        paper_abstract = paper.summary.replace("\n", " ")  # 摘要（去除换行）
        all_authors = format_authors(paper.authors)  # 所有作者
        first_author = format_authors(paper.authors, only_first_author=True)  # 第一作者
        primary_category = paper.primary_category  # 主要分类
        publish_date = paper.published.date()  # 发表日期
        update_date = paper.updated.date()  # 更新日期
        paper_comments = paper.comment  # 论文备注（如页数、会议）
        
        logging.info(f"发现论文：{update_date} | {paper_title} | 作者：{first_author}")
        
        # -------------------------- 2. 处理论文ID（去除版本号，如 "v1"） --------------------------
        version_pos = raw_paper_id.find("v")  # 查找版本号起始位置
        clean_paper_id = raw_paper_id[:version_pos] if version_pos != -1 else raw_paper_id
        clean_arxiv_url = f"{ARXIV_BASE_URL}abs/{clean_paper_id}"  # 纯净的论文详情页链接
        
        # -------------------------- 3. 尝试获取代码链接（优先PapersWithCode，备用GitHub） --------------------------
        code_url = None
        # 1. 先请求PapersWithCode API（官方代码链接更可靠）
        papers_with_code_api = f"{PAPERS_WITH_CODE_BASE_URL}{clean_paper_id}"
        
        try:
            response = requests.get(papers_with_code_api)
            response.raise_for_status()
            pwc_data = response.json()
            
            # 若有官方代码链接，直接使用
            if "official" in pwc_data and pwc_data["official"]:
                code_url = pwc_data["official"]["url"]
            
            # TODO：原代码预留的备用逻辑（PapersWithCode无结果时搜GitHub）
            # elif code_url is None:
            #     code_url = search_github_code(paper_title)
            #     if code_url is None:
            #         code_url = search_github_code(clean_paper_id)
        
        except Exception as e:
            logging.error(f"PapersWithCode API请求失败（论文ID：{clean_paper_id}），错误：{e}")
        
        # -------------------------- 4. 构造Markdown格式的数据 --------------------------
        # 表格格式（用于README/GitPage）
        if code_url:
            table_row = (
                f"|**{update_date}**|**{paper_title}**|{first_author}|[{clean_paper_id}]({clean_arxiv_url})|**[link]({code_url})**|\n"
            )
        else:
            table_row = (
                f"|**{update_date}**|**{paper_title}**|{first_author}|[{clean_paper_id}]({clean_arxiv_url})|null|\n"
            )
        markdown_table_data[clean_paper_id] = table_row
        
        # 列表格式（用于微信推送）
        if code_url:
            list_item = (
                f"- {update_date}, **{paper_title}**, {first_author}, Paper: [{clean_arxiv_url}]({clean_arxiv_url}), Code: **[{code_url}]({code_url})**"
            )
        else:
            list_item = (
                f"- {update_date}, **{paper_title}**, {first_author}, Paper: [{clean_arxiv_url}]({clean_arxiv_url})"
            )
        # 补充论文备注（若有）
        if paper_comments:
            list_item += f", {paper_comments}\n"
        else:
            list_item += "\n"
        markdown_list_data[clean_paper_id] = list_item
    
    return {topic: markdown_table_data}, {topic: markdown_list_data}


def update_paper_code_links(json_file_path: str) -> None:
    """
    批量更新JSON文件中已存储论文的代码链接（用于定期补全缺失的代码链接）
    
    逻辑：解析JSON中存储的Markdown表格行，重新请求PapersWithCode API获取最新代码链接
    Args:
        json_file_path: 存储论文数据的JSON文件路径
    """
    def parse_paper_info_from_markdown(markdown_row: str) -> tuple[str, str, str, str, str]:
        """辅助函数：从Markdown表格行中解析论文关键信息"""
        parts = [part.strip() for part in markdown_row.split("|")]
        # parts格式：["", 更新日期, 标题, 作者, 论文ID链接, 代码链接, ""]
        update_date = parts[1]
        title = parts[2]
        authors = parts[3]
        # 从 "[2108.09112](url)" 中提取纯净论文ID
        paper_id = re.sub(r'v\d+', '', re.findall(r'\[(.*?)\]', parts[4])[0])
        code_link = parts[5]
        return update_date, title, authors, paper_id, code_link
    
    # 1. 读取现有JSON数据
    with open(json_file_path, "r", encoding="utf-8") as f:
        content = f.read()
        paper_data = json.loads(content) if content else {}
    
    # 2. 遍历每篇论文，更新代码链接
    for topic, papers in paper_data.items():
        logging.info(f"开始更新主题「{topic}」的论文代码链接")
        for paper_id, markdown_row in papers.items():
            # 解析现有论文信息
            update_date, title, authors, clean_paper_id, old_code_link = parse_paper_info_from_markdown(markdown_row)
            
            # 若原有代码链接为空（null），尝试重新获取
            if old_code_link == "null":
                try:
                    # 重新请求PapersWithCode API
                    pwc_api = f"{PAPERS_WITH_CODE_BASE_URL}{clean_paper_id}"
                    response = requests.get(pwc_api)
                    response.raise_for_status()
                    pwc_data = response.json()
                    
                    if "official" in pwc_data and pwc_data["official"]:
                        new_code_link = f"**[link]({pwc_data['official']['url']})**"
                        # 替换Markdown行中的null为新链接
                        new_markdown_row = markdown_row.replace("|null|", f"|{new_code_link}|")
                        paper_data[topic][paper_id] = new_markdown_row
                        logging.info(f"论文ID {clean_paper_id} 成功补全代码链接")
                
                except Exception as e:
                    logging.error(f"更新论文ID {clean_paper_id} 代码链接失败，错误：{e}")
    
    # 3. 写回更新后的JSON数据
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(paper_data, f, indent=2)


def update_papers_json_file(json_file_path: str, new_papers_data: list[dict]) -> None:
    """
    将新爬取的论文数据更新到JSON文件中（增量更新，不覆盖原有数据）
    
    Args:
        json_file_path: 目标JSON文件路径
        new_papers_data: 新爬取的论文数据列表（每个元素为{topic: 论文字典}）
    """
    # 1. 读取现有JSON数据
    with open(json_file_path, "r", encoding="utf-8") as f:
        content = f.read()
        existing_data = json.loads(content) if content else {}
    
    # 2. 增量更新：添加新论文（若论文ID已存在，会覆盖旧数据）
    for new_topic_data in new_papers_data:
        for topic, new_papers in new_topic_data.items():
            if topic in existing_data:
                # 若主题已存在，更新论文字典
                existing_data[topic].update(new_papers)
            else:
                # 若主题不存在，新建主题条目
                existing_data[topic] = new_papers
    
    # 3. 写回更新后的数据
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2)


def convert_json_to_markdown(json_file_path: str, md_file_path: str, 
                            task_name: str = "", to_web: bool = False,
                            use_title: bool = True, use_toc: bool = True,
                            show_badge: bool = True, use_back_to_top: bool = True) -> None:
    """
    将JSON中的论文数据转换为Markdown文件（支持README、GitPage、微信推送等多种格式）
    
    Args:
        json_file_path: 存储论文数据的JSON文件路径
        md_file_path: 输出的Markdown文件路径
        task_name: 任务名称（用于日志）
        to_web: 是否为GitPage生成（需适配网页布局）
        use_title: 是否添加标题和更新时间
        use_toc: 是否生成目录
        show_badge: 是否显示GitHub徽章（星数、分支等）
        use_back_to_top: 是否添加「返回顶部」链接
    """
    def format_latex_formula(text: str) -> str:
        """辅助函数：优化Markdown中的LaTeX公式格式（添加必要空格）"""
        # 匹配 $...$ 格式的公式
        formula_match = re.search(r"\$.*\$", text)
        if not formula_match:
            return text
        
        formula_start, formula_end = formula_match.span()
        leading_space = ""
        trailing_space = ""
        
        # 公式前若不是空格或星号，加空格
        if formula_start > 0 and text[formula_start - 1] not in (" ", "*"):
            trailing_space = " "
        # 公式后若不是空格或星号，加空格
        if formula_end < len(text) and text[formula_end] not in (" ", "*"):
            leading_space = " "
        
        # 重构文本：原文本前半部分 + 优化后的公式 + 原文本后半部分
        return (
            text[:formula_start]
            + f"{trailing_space}${formula_match.group()[1:-1].strip()}${leading_space}"
            + text[formula_end:]
        )
    
    # 1. 读取JSON数据
    with open(json_file_path, "r", encoding="utf-8") as f:
        content = f.read()
        paper_data = json.loads(content) if content else {}
    
    # 2. 获取当前日期（用于标题）
    current_date = datetime.date.today().strftime("%Y.%m.%d")
    
    # 3. 清空原有Markdown文件，准备写入新内容
    with open(md_file_path, "w", encoding="utf-8") as f:
        pass
    
    # 4. 写入Markdown内容
    with open(md_file_path, "a", encoding="utf-8") as f:
        # 若为GitPage，添加Jekyll布局头
        if to_web and use_title:
            f.write("---\nlayout: default\n---\n\n")
        
        # 写入标题和更新时间
        if use_title:
            f.write(f"## Updated on {current_date}\n")
        else:
            f.write(f"> Updated on {current_date}\n")
        
        # 写入使用说明链接
        f.write("> Usage instructions: [here](./docs/README.md#usage)\n\n")
        
        # 生成目录（Table of Contents）
        if use_toc:
            f.write("<details>\n  <summary>Table of Contents</summary>\n  <ol>\n")
            for topic in paper_data.keys():
                # 目录链接格式：将主题名转为小写+连字符（如 "SLAM" → "#slam"）
                topic_link = topic.replace(" ", "-").lower()
                f.write(f"    <li><a href=#{topic_link}>{topic}</a></li>\n")
            f.write("  </ol>\n</details>\n\n")
        
        # 写入每个主题的论文列表
        for topic, papers in paper_data.items():
            if not papers:  # 若该主题无论文，跳过
                continue
            
            # 主题标题
            f.write(f"## {topic}\n\n")
            
            # 若为表格格式（README/GitPage），写入表头
            if use_title and not to_web:
                f.write("|Publish Date|Title|Authors|PDF|Code|\n|---|---|---|---|---|\n")
            elif use_title and to_web:
                f.write("| Publish Date | Title | Authors | PDF | Code |\n|:---------|:-----------------------|:---------|:------|:------|\n")
            
            # 按论文ID倒序排序（最新在前）
            sorted_papers = sort_papers_by_id_desc(papers)
            
            # 写入每篇论文的信息（优化LaTeX格式）
            for _, markdown_content in sorted_papers.items():
                if markdown_content:
                    f.write(format_latex_formula(markdown_content))
            
            f.write("\n")
            
            # 添加「返回顶部」链接
            if use_back_to_top:
                top_link = f"#updated-on-{current_date.replace('.', '')}"
                f.write(f"<p align=right>(<a href={top_link}>back to top</a>)</p>\n\n")
        
        # 显示GitHub徽章（星数、分支等）
        if show_badge:
            badge_template = (
                "[contributors-shield]: https://img.shields.io/github/contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"
                "[contributors-url]: https://github.com/Vincentqyw/cv-arxiv-daily/graphs/contributors\n"
                "[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"
                "[forks-url]: https://github.com/Vincentqyw/cv-arxiv-daily/network/members\n"
                "[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"
                "[stars-url]: https://github.com/Vincentqyw/cv-arxiv-daily/stargazers\n"
                "[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"
                "[issues-url]: https://github.com/Vincentqyw/cv-arxiv-daily/issues\n\n"
            )
            f.write(badge_template)
    
    logging.info(f"任务「{task_name}」完成：已生成Markdown文件 {md_file_path}")


# -------------------------- 主工作流程 --------------------------
def main_workflow(config: dict) -> None:
    """
    主工作流程：根据配置决定「爬取新论文」或「更新旧论文代码链接」，并生成对应Markdown
    
    Args:
        config: 完整配置字典（含格式化关键词、文件路径、功能开关等）
    """
    # 从配置中提取关键参数（避免重复写config["key"]）
    formatted_keywords = config["formatted_keywords"]
    max_results = config["max_results"]
    update_only_links = config["update_paper_links"]  # 是否仅更新代码链接，不爬新论文
    
    # 功能开关
    publish_readme = config["publish_readme"]
    publish_gitpage = config["publish_gitpage"]
    publish_wechat = config["publish_wechat"]
    show_github_badge = config["show_badge"]
    
    # 存储新爬取的论文数据
    new_table_data = []  # 表格格式（README/GitPage）
    new_list_data = []   # 列表格式（微信）
    
    # -------------------------- 步骤1：爬取新论文 或 仅更新代码链接 --------------------------
    if not update_only_links:
        logging.info("开始爬取arXiv每日新论文...")
        # 遍历每个主题，爬取对应论文
        for topic, search_query in formatted_keywords.items():
            logging.info(f"正在爬取主题：{topic}（搜索关键词：{search_query}）")
            # 爬取论文，获取两种格式的数据
            table_data, list_data = fetch_daily_arxiv_papers(
                topic=topic,
                search_query=search_query,
                max_results=max_results
            )
            new_table_data.append(table_data)
            new_list_data.append(list_data)
        logging.info("新论文爬取完成！")
    else:
        logging.info("启用「仅更新代码链接」模式，不爬取新论文")
    
    # -------------------------- 步骤2：更新README.md（本地文档） --------------------------
    if publish_readme:
        json_path = config["json_readme_path"]
        md_path = config["md_readme_path"]
        
        if update_only_links:
            # 仅更新代码链接
            update_paper_code_links(json_path)
        else:
            # 增量更新新论文到JSON
            update_papers_json_file(json_path, new_table_data)
        
        # 转换JSON为Markdown
        convert_json_to_markdown(
            json_file_path=json_path,
            md_file_path=md_path,
            task_name="更新README",
            show_badge=show_github_badge,
            use_toc=True,
            use_back_to_top=True
        )
    
    # -------------------------- 步骤3：更新GitPage（网页展示） --------------------------
    if publish_gitpage:
        json_path = config["json_gitpage_path"]
        md_path = config["md_gitpage_path"]
        
        if update_only_links:
            update_paper_code_links(json_path)
        else:
            update_papers_json_file(json_path, new_table_data)
        
        convert_json_to_markdown(
            json_file_path=json_path,
            md_file_path=md_path,
            task_name="更新GitPage",
            to_web=True,  # 适配网页布局
            show_badge=show_github_badge,
            use_toc=False,  # 网页可能不需要目录
            use_back_to_top=False
        )
    
    # -------------------------- 步骤4：更新微信推送文档 --------------------------
    if publish_wechat:
        json_path = config["json_wechat_path"]
        md_path = config["md_wechat_path"]
        
        if update_only_links:
            update_paper_code_links(json_path)
        else:
            # 微信用列表格式的数据
            update_papers_json_file(json_path, new_list_data)
        
        convert_json_to_markdown(
            json_file_path=json_path,
            md_file_path=md_path,
            task_name="更新微信推送",
            use_title=False,  # 微信推送不需要大标题
            show_badge=show_github_badge,
            use_toc=False,
            use_back_to_top=False
        )


# -------------------------- 程序入口 --------------------------
if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="arXiv论文每日爬取与Markdown生成工具")
    parser.add_argument(
        "--config_path",
        type=str,
        default="config.yaml",
        help="配置文件路径（默认：config.yaml）"
    )
    parser.add_argument(
        "--update_paper_links",
        action="store_true",
        default=False,
        help="是否仅更新论文代码链接，不爬取新论文（用于定期补全链接）"
    )
    args = parser.parse_args()
    
    # 加载配置 + 合并命令行参数（命令行参数优先级高于配置文件）
    config = load_config(args.config_path)
    config["update_paper_links"] = args.update_paper_links  # 覆盖配置文件中的开关
    
    # 启动主工作流程
    main_workflow(config)