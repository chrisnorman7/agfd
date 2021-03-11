"""The audiogames.net forum downloader."""

from datetime import datetime
from typing import Iterator, List, Optional, Union

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from html2markdown import convert
from requests import Response
from requests import Session as RequestsSession
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.orm import Session as OrmSession
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.relationships import RelationshipProperty

FindType = Union[Tag, NavigableString]
engine: Engine = create_engine("sqlite:///db.sqlite3")


class _BaseClass:
    """Add an ID."""

    __tablename__: str
    id = Column(Integer, primary_key=True)

    def save(self) -> None:
        """Save this instance."""
        session.add(self)
        session.commit()

    @classmethod
    def query(cls, *args, **kwargs) -> Query:
        """Return a query linked to this class."""
        return session.query(cls).filter(*args).filter_by(**kwargs)

    @classmethod
    def count(cls, *args, **kwargs) -> int:
        """Return the number of rows that match the given criteria."""
        return cls.query(*args, **kwargs).count()


Base = declarative_base(bind=engine, cls=_BaseClass)


Session = sessionmaker(bind=engine)
session: OrmSession = Session()


class NameMixin:
    """Add a name parameter."""

    id: int

    name = Column(String(1024), nullable=False)

    def __str__(self) -> str:
        """Return a string representation of this object."""
        return f"{self.name} (#{self.id})"


class User(Base, NameMixin):  # type:ignore[valid-type, misc]
    """A forum user."""

    __tablename__ = "users"
    name = Column(String(50), nullable=False)
    registered = Column(DateTime(timezone=True), nullable=True)


class Room(Base, NameMixin):  # type:ignore[valid-type, misc]
    """A room in the forum."""

    __tablename__ = "rooms"


class Thread(Base, NameMixin):  # type:ignore[valid-type, misc]
    """A forum thread."""

    __tablename__ = "threads"
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    user: RelationshipProperty = relationship("User", backref="threads")
    room_id = Column(Integer, ForeignKey("rooms.id"), nullable=False)
    room: RelationshipProperty = relationship("Room", backref="threads")


class Post(Base):  # type:ignore[valid-type, misc]
    """A forum post."""

    __tablename__ = "posts"
    posted = Column(DateTime(timezone=True), nullable=False)
    text = Column(String(65535), nullable=True)
    url = Column(String(1024), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    user: RelationshipProperty = relationship("User", backref="posts")
    thread_id = Column(Integer, ForeignKey("threads.id"), nullable=False)
    thread: RelationshipProperty = relationship("Thread", backref="posts")


Base.metadata.create_all()
url: str = "https://forum.audiogames.net/"
http: RequestsSession = RequestsSession()


def main() -> None:
    """Start scraping."""
    response: Response = http.get(url)
    soup: BeautifulSoup = BeautifulSoup(response.text, "lxml")
    h3: Tag
    tags: Iterator[FindType] = soup.find_all("h3")
    for h3 in tags:
        assert isinstance(h3, Tag)
        parse_room(h3)


def parse_room(h3: Tag) -> None:
    """Parse a room from a link.

    :param h3: The level 3 heading containing the link from the main forum.
    """
    a: Optional[FindType] = h3.find("a")
    if a is None or isinstance(a, NavigableString):
        raise RuntimeError("Invalid room link:\n%s" % h3)
    href: str = a["href"]
    name: str = a.text
    room: Optional[Room] = Room.query(name=name).first()
    if room is None:
        room = Room(name=a.text)
        room.save()
        print(f"Created room {room.name}.")
    else:
        print(f"Using existing room {room}.")
    response = http.get(href)
    soup = BeautifulSoup(response.text, "lxml")
    p: Optional[FindType] = soup.find("p", attrs={"class": "paging"})
    if p is None or isinstance(p, NavigableString):
        return print("Cannot find page links for this room.")
    links: List[FindType] = p.find_all("a")
    a = links[-2]
    assert isinstance(a, Tag)
    parse_pages(room, a)


def parse_pages(room: Room, a: Tag) -> None:
    """Parse pages of threads for a particular room.

    :param room: The room to work in.

    :param a: The link to the page with the highest number.
    """
    href = a["href"][:-1]
    href = href[: href.rindex("/") + 1] + "%d"
    page: int = int(a.text)
    while page > 0:
        print(f"Parsing page {page}.")
        response = http.get(href % page)
        soup = BeautifulSoup(response.text, "lxml")
        tags = soup.find_all("h3")
        for h3 in tags:
            assert isinstance(h3, Tag)
            parse_thread(room, h3)
        room.save()
        page -= 1


def parse_thread(room: Room, h3: Tag) -> None:
    """Parse a particular thread in the given room.

    :param room: The room to work in.

    :param h3: The level 3 heading containing the link to the thread to parse.
    """
    a = h3.find("a")
    assert isinstance(a, Tag)
    name = a.text
    href = a["href"]
    thread: Optional[Thread] = Thread.query(name=name, room=room).first()
    if thread is None:
        thread = Thread(name=name, room=room)
    response = http.get(href)
    soup = BeautifulSoup(response.text, "lxml")
    tags = soup.find_all("div", attrs={"class": "post"})
    div: Tag
    for div in tags:
        assert isinstance(div, Tag)
        parse_message(thread, div)


def parse_message(thread: Thread, div: Tag) -> None:
    """Parse the given message.

    :param thread: The thread this message will belong to.

    :param div: The div element containing the message to parse.
    """
    href = div.find_all("a")[0]["href"]
    post_id: str = href[len(url) :]
    post_id = post_id[len("post/") :].split("/")[0]
    if Post.count(id=post_id) > 0:
        return print(f"Skipping message #{post_id}.")
    span: Optional[FindType] = div.find("span", attrs={"class": "post-byline"})
    assert isinstance(span, Tag)
    username: str = span.find("strong").text
    user: Optional[User] = User.query(name=username).first()
    if user is None:
        print(f"Creating user {username}.")
        ul: Optional[FindType] = div.find("ul", attrs={"class": "author-info"})
        assert isinstance(ul, Tag)
        li: Optional[FindType] = ul.find(
            lambda t: t.name == "span" and t.text.startswith("Registered:")
        )
        registered: Optional[datetime] = None
        if li is not None:
            registered = datetime.fromisoformat(li.find("strong").text)
        user = User(name=username, registered=registered)
        user.save()
    else:
        print(f"Using existing user {user}.")
    if "firstpost" in div["class"]:
        print(f"{username} is thread starter.")
        thread.user = user
        thread.save()
    content: Optional[FindType] = div.find("div", attrs={"class": "entry-content"})
    assert isinstance(content, Tag)
    signature: Optional[FindType] = content.find("div")
    span = div.find("span", attrs={"class": "post-link"})
    assert isinstance(span, Tag)
    posted: datetime = datetime.fromisoformat(span.text)
    strings: List[str] = []
    child: FindType
    for child in content:
        if isinstance(child, NavigableString):
            continue
        if child is not signature:
            strings.append(convert(str(child)))
    post: Post = Post(
        id=int(
            post_id,
        ),
        posted=posted,
        text="\n\n".join(strings),
        user=user,
        thread=thread,
        url=href,
    )
    print(f"Created post #{post_id}.")
    post.save()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        session.rollback()
        print("Aborted.")
    finally:
        print(f"Users: {User.count()}")
        print(f"Threads: {Thread.count()}")
        print(f"Posts: {Post.count()}.")
        session.close()
