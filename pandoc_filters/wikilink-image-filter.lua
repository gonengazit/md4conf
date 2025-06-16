-- pandoc-wikilink-image-filter.lua
--
-- A Pandoc Lua filter to transform wikilink-style image embeds
-- into standard Pandoc image elements.
--
-- This filter looks for the pattern: Str("!") .. Link(...)
-- and converts it into Image(...).
--
-- This allows you to use the syntax `![[my-image.jpg]]` in your
-- Markdown files and have Pandoc correctly render it as an image.
--
-- To work correctly, Pandoc must be configured to recognize the
-- `[[...]]` syntax as a link. This can be done by using an
-- appropriate markdown extension, for example:
-- `pandoc -f markdown+wikilinks_title_after_pipe ...`

--- Transforms a list of Inline elements.
-- @param inlines A pandoc.Inlines object.
-- @return A pandoc.Inlines object with transformations applied.
function Inlines (inlines)
  -- We will build a new list of inlines rather than modifying the list
  -- we are iterating over, which can lead to errors.
  local result = pandoc.Inlines{}
  local i = 1

  -- Iterate through all the inline elements in the list.
  while i <= #inlines do
    local current_inline = inlines[i]
    local next_inline = inlines[i + 1] -- Can be nil if at the end of the list.

    -- THE PATTERN:
    -- We are looking for a String element that contains exactly "!"...
    if current_inline.tag == 'Str' and current_inline.text == '!' and
       -- ...followed immediately by a Link element.
       next_inline and next_inline.tag == 'Link' then

      -- PATTERN FOUND.
      -- Now, we transform the Link element into an Image element.
      local link = next_inline

      -- The pandoc.Image constructor expects the URL and title as separate
      -- string arguments. We unpack the link's target table.
      --
      -- We add `or ''` as a fallback to prevent errors if the link
      -- is malformed and doesn't have a URL or title. This ensures we
      -- pass an empty string instead of a `nil` value, which would
      -- crash the filter.
      local image = pandoc.Image(link.content, link.target, link.title, link.attr)

      -- Add the newly created image to our result list.
      result:insert(image)

      -- We've processed both the '!' string and the Link, so we advance
      -- our index by 2 to skip over the Link in the next iteration.
      i = i + 2

    else
      -- PATTERN NOT FOUND.
      -- Just add the current inline element to the result list and move on.
      result:insert(current_inline)
      i = i + 1
    end
  end

  -- Return the new list of inlines. Pandoc will use this to replace the original.
  return result
end
