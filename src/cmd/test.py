from os import getenv

from nextcord import Interaction, slash_command
from nextcord.ext import commands

import src.md as md


class TestCog(commands.Cog):
    '''Cog that handles Test commands.'''

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    # @slash_command(
    #     description='test all markdown functions',
    #     guild_ids=[int(getenv('TEST_SERVER'))]
    # )
    # async def markdown(self, interaction: Interaction):
    #     await interaction.response.send_message('Testing markdown...')

    #     async def send_test(name: str, content: str) -> None:
    #         await interaction.followup.send(md.bold(name+':')+'\n'+content)

    #     # Test strings and escaped text
    #     await send_test('Escape', md.escape(md.bold('Bold?')))
    #     await send_test('Italic', md.italic('This is italic'))
    #     await send_test('Bold', md.bold('This is bold'))
    #     await send_test('Underline', md.underline('Underlined text'))
    #     await send_test('Strike', md.strike('Strikethrough text'))

    #     # Headers
    #     await send_test('Header 1', md.header('Header 1', md.HEADER_BIG))
    #     await send_test('Header 2', md.header('Header 2', md.HEADER_SMALL))
    #     await send_test('Header 3', md.header('Header 3', md.HEADER_SMALLER))

    #     # Subtext
    #     await send_test('Subtext', md.subtext('This is subtext'))

    #     # Hyperlinks
    #     await send_test('Hyperlink Normal', md.hyperlink('Click me', 'https://www.youtube.com/watch?v=KcS9mu52__w'))
    #     await send_test('Hyperlink Embed', md.hyperlink('Click me', 'https://www.youtube.com/watch?v=KcS9mu52__w', embed=True))

    #     # Monospace and code block
    #     await send_test('Monospace', md.monospace('inline code'))
    #     await send_test('Code Block', md.code("print('Hello')", lang='py'))

    #     # Quote and spoiler
    #     await send_test('Quote', md.quote('This is a quote'))
    #     await send_test('Spoiler', md.spoiler('Hidden text'))

    #     # Lists - nested, mixed types
    #     nested_data = {
    #         'Fruits': ['Apple', 'Banana', {'Citrus': ('Orange', 'Lime')}],
    #         'Numbers': [1, 2, [3, 4]],
    #         'Info': '  Keep refrigerated\n    Consume soon',
    #         'Mixed': [{'A': 1}, ['Nested list'], 'String item']
    #     }

    #     await send_test('Unordered List', md.format_list(nested_data))
    #     await send_test('Ordered List', md.format_list(nested_data, True))
    #     await send_test('Ordered List', md.format_list(nested_data, True, start=5))

    @slash_command(
        description='test all markdown functions',
        guild_ids=[int(getenv('TEST_SERVER'))]
    )
    async def markdown(self, interaction: Interaction) -> None:
        msg = []

        # --- escape ---
        msg.append(f"{md.bold('Escape with number:')}\n{md.escape(123)}")
        msg.append(f"{md.bold('Escape with list:')}\n{md.escape([1,2,3])}")

        # --- italic ---
        msg.append(f"{md.bold('Italic with number:')}\n{md.italic(42)}")
        msg.append(f"{md.bold('Italic nested with bold:')}\n{md.italic(md.bold(3.14))}")

        # --- bold ---
        msg.append(f"{md.bold('Bold with list:')}\n{md.bold([1,'a',3])}")
        msg.append(f"{md.bold('Bold nested with underline:')}\n{md.bold(md.underline(('tuple', 2)))}")

        # --- underline ---
        msg.append(f"{md.bold('Underline with dict:')}\n{md.underline({'key': 'value'})}")
        msg.append(f"{md.bold('Underline nested with strike:')}\n{md.underline(md.strike(999))}")

        # --- strike ---
        msg.append(f"{md.bold('Strike with bool:')}\n{md.strike(True)}")
        msg.append(f"{md.bold('Strike nested with italic:')}\n{md.strike(md.italic(False))}")

        # --- header ---
        msg.append(f"{md.bold('Header Big with int:')}\n{md.header(123, md.HEADER_BIG)}")
        msg.append(f"{md.bold('Header Small with tuple:')}\n{md.header(('tuple', 1), md.HEADER_SMALL)}")

        # --- subtext ---
        msg.append(f"{md.bold('Subtext with dict:')}\n{md.subtext({'a': 1})}")

        # --- hyperlink ---
        msg.append(f"{md.bold('Hyperlink with int text:')}\n{md.hyperlink(12345, 'https://example.com')}")
        msg.append(f"{md.bold('Hyperlink with nested formatting:')}\n{md.hyperlink(md.bold('Bold Link'), 'https://example.com')}")

        # --- monospace ---
        msg.append(f"{md.bold('Monospace with int:')}\n{md.monospace(123)}")
        msg.append(f"{md.bold('Monospace nested with bold:')}\n{md.monospace(md.bold('Text'))}")

        # --- code ---
        msg.append(f"{md.bold('Code block with int:')}\n{md.code(42)}")
        msg.append(f"{md.bold('Code block with list:')}\n{md.code([1,2,3], lang='python')}")

        # --- quote ---
        # msg.append(f"{md.bold('Quote with number:')} {md.quote(12345)}")
        # msg.append(f"{md.bold('Quote nested with bold:')} {md.quote(md.bold('Quoted'))}")

        # --- spoiler ---
        msg.append(f"{md.bold('Spoiler with int:')}\n{md.spoiler(99)}")
        msg.append(f"{md.bold('Spoiler nested with italic:')}\n{md.spoiler(md.italic('Hidden'))}")

        # --- format_list with nested/mixed data ---
        nested_data = {
            'Fruits': ['Apple', 42, {'Citrus': ('Orange', 3.14)}],
            'Numbers': [1, 2, [3, 4]],
            'Info': '  Keep refrigerated\n    Consume soon',
            'Mixed': [{'A': 1}, ['Nested list'], 42]
        }
        msg.append(f"{md.bold('Unordered List with nested structures:')}\n{md.format_list(nested_data)}")
        msg.append(f"{md.bold('Ordered List starting at 5:')}\n{md.format_list(nested_data, ordered=True, start=5)}")

        # --- Nested functions together ---
        complex_nesting = md.bold(
            md.italic(
                md.underline(
                    md.strike(
                        md.escape([1, 2, 3])
                    )
                )
            )
        )
        msg.append(f"{md.bold('Complex nested formatting on list:')}\n{complex_nesting}")

        # --- Nested format_list with formatting inside ---
        nested_list = [
            md.bold('Bold Item'),
            md.format_list([md.italic('Italic nested in list'), md.strike('Strike inside list')]),
            {'Key': md.format_list([md.underline('Underlined list item 1'), 42, ('tuple', 'item')])}
        ]
        msg.append(f"{md.bold('Complex nested list with formatting inside:')}\n{md.format_list(nested_list, ordered=True)}")

        # Send everything as a single message
        await interaction.response.send_message('\n\n'.join(msg))


def setup(bot: commands.Bot):
    bot.add_cog(TestCog(bot))
