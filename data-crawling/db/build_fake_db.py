import uuid

from faker import Faker

from db.documents import RepositoryDocument, UserDocument, PostDocument, ArticleDocument

fake = Faker()


def generate_fake_data(num_users=100, num_repos=100, num_posts=100, num_articles=100):
    users = []
    repos = []
    posts = []
    articles = []

    for _ in range(num_users):
        user = UserDocument(first_name=fake.first_name(), last_name=fake.last_name())
        users.append(user)

    for _ in range(num_repos):
        repo = RepositoryDocument(
            name=fake.word(),
            link=fake.url(),
            content={"description": fake.text()},
            owner_id=str(uuid.uuid4()),
        )
        repos.append(repo)

    for _ in range(num_posts):
        post = PostDocument(
            platform=fake.word(),
            content={"text": fake.text()},
            author_id=str(uuid.uuid4()),
        )
        posts.append(post)

    for _ in range(num_articles):
        article = ArticleDocument(
            platform=fake.word(),
            link=fake.url(),
            content={"text": fake.text()},
            author_id=str(uuid.uuid4()),
        )
        articles.append(article)

    UserDocument.bulk_insert(users)
    RepositoryDocument.bulk_insert(repos)
    PostDocument.bulk_insert(posts)
    ArticleDocument.bulk_insert(articles)


if __name__ == "__main__":
    generate_fake_data(num_users=1, num_repos=1, num_posts=1, num_articles=1)
    print("Fake data generation completed.")
