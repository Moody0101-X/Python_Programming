import pygame
class code:
    """Just an initializer, But running the Instance is manual."""
    def __init__(self, Title, Backgroundcolor=(255, 255, 255), width=None, height=None):
        self.Title = Title
        self.width = width
        self.height = height
        if not self.height:
            self.height = 500
        if not self.width:
            self.width = 500
        self.clock = pygame.time.Clock()
        pygame.init()
        pygame.display.set_caption(self.Title)
        self.Backgroundcolor = Backgroundcolor
        self.window = pygame.display.set_mode((self.width, self.height))
        self.window.fill(self.Backgroundcolor)
        self.run = True
